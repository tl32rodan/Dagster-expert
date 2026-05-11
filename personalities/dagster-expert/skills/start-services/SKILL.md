---
name: start-services
description: How to start dagster-webserver, dagster-daemon, and code servers in dev (dagster dev) and production (separate processes, ideally via systemd).
---

<!-- all-might generated -->

# start-services — start the Dagster processes

## When to use

- User asks "how do I start dagster?"
- User asks for a systemd / service config
- User asks the difference between `dagster dev` and prod startup

## The processes you actually run

| Process | What it does | Cardinality |
|---|---|---|
| `dagster-webserver` | UI + GraphQL | 1+ (behind a load balancer for HA — rare) |
| `dagster-daemon` | Schedules, sensors, run queue, run monitor, backfills | **Exactly 1** per instance |
| `dagster code-server start` | gRPC code location | 1 per code location |
| `dagster dev` | All of the above in one process | dev only |

**Hard rule**: never run two `dagster-daemon` processes against
the same instance. Schedules will double-fire and run-coordination
state corrupts.

## Dev mode — `dagster dev` (one command, do not use in prod)

```bash
source ~/dagster-venv/bin/activate
export DAGSTER_HOME=~/.dagster
mkdir -p $DAGSTER_HOME

cd ~/projects/my_pipelines
dagster dev -w workspace.yaml -p 3000
```

This starts webserver + daemon + code servers in one process,
opens UI on http://localhost:3000. Press Ctrl-C to stop.

Limitations:
- Single host, single user
- No survivability (Ctrl-C kills everything)
- SQLite-only is the realistic backing store for `dagster dev`
- Code location runs in-process — bad import crashes the whole dev server

## Production — three separate processes

### 1. Code servers (one per location)

```bash
# As the dagster service user
source ~/dagster-venv/bin/activate
export DAGSTER_HOME=/var/lib/dagster

dagster code-server start \
    -m my_pipelines \
    --host 0.0.0.0 \
    --port 4000 \
    --location-name pipelines
```

Repeat per location with different `--port` and `--location-name`.

### 2. Webserver

```bash
source ~/dagster-venv/bin/activate
export DAGSTER_HOME=/var/lib/dagster
export DAGSTER_PG_PASSWORD="$(cat /run/secrets/dagster_pg)"

dagster-webserver \
    -w /etc/dagster/workspace.yaml \
    -h 0.0.0.0 \
    -p 3000
```

The webserver does **not** authenticate. Front it with nginx /
Caddy for TLS + auth before exposing on a network anyone can
reach.

### 3. Daemon

```bash
source ~/dagster-venv/bin/activate
export DAGSTER_HOME=/var/lib/dagster
export DAGSTER_PG_PASSWORD="$(cat /run/secrets/dagster_pg)"

dagster-daemon run
```

`dagster-daemon liveness-check` exits 0 if healthy — use this as
your health probe.

### Startup order

1. Postgres up
2. `dagster instance migrate` (once after install/upgrade)
3. Code servers up (each on its port)
4. Daemon up
5. Webserver up

The webserver and daemon will retry connecting to code servers,
so order 3↔4↔5 is forgiving in practice. But code servers MUST
be up before any UI/run interaction works against them.

## systemd unit examples

### `/etc/systemd/system/dagster-code-pipelines.service`

```ini
[Unit]
Description=Dagster code server (pipelines)
After=network.target

[Service]
Type=simple
User=dagster
Group=dagster
Environment=DAGSTER_HOME=/var/lib/dagster
EnvironmentFile=/etc/dagster/dagster.env
WorkingDirectory=/opt/my_pipelines
ExecStart=/opt/dagster-venv/bin/dagster code-server start \
    -m my_pipelines \
    --host 0.0.0.0 \
    --port 4000 \
    --location-name pipelines
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### `/etc/systemd/system/dagster-webserver.service`

```ini
[Unit]
Description=Dagster webserver
After=network.target dagster-code-pipelines.service

[Service]
Type=simple
User=dagster
Group=dagster
Environment=DAGSTER_HOME=/var/lib/dagster
EnvironmentFile=/etc/dagster/dagster.env
ExecStart=/opt/dagster-venv/bin/dagster-webserver \
    -w /etc/dagster/workspace.yaml \
    -h 0.0.0.0 \
    -p 3000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### `/etc/systemd/system/dagster-daemon.service`

```ini
[Unit]
Description=Dagster daemon
After=network.target dagster-code-pipelines.service

[Service]
Type=simple
User=dagster
Group=dagster
Environment=DAGSTER_HOME=/var/lib/dagster
EnvironmentFile=/etc/dagster/dagster.env
ExecStart=/opt/dagster-venv/bin/dagster-daemon run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### `/etc/dagster/dagster.env`

```
DAGSTER_PG_PASSWORD=<the password>
# Plus any other env vars your assets need
```

`chmod 600 /etc/dagster/dagster.env` and own as `dagster:dagster`.

### Enabling

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dagster-code-pipelines
sudo systemctl enable --now dagster-webserver
sudo systemctl enable --now dagster-daemon
```

## Smoke test after starting

```bash
# All three should be active
systemctl status dagster-code-pipelines dagster-webserver dagster-daemon

# Daemon healthy?
sudo -u dagster /opt/dagster-venv/bin/dagster-daemon liveness-check

# Webserver up?
curl -fsS http://localhost:3000/server_info

# Code location loads?
sudo -u dagster -E /opt/dagster-venv/bin/dagster definitions validate \
    -w /etc/dagster/workspace.yaml
```

If any fail, see `skills/verify-deploy/SKILL.md` for the full
diagnosis chain.

## Restart vs reload

| What changed | What to restart |
|---|---|
| Asset / op Python code | The code server for that location only |
| `workspace.yaml` | Webserver + daemon |
| `dagster.yaml` | Webserver + daemon (and code servers if env-dependent) |
| Dagster version | Everything, then `dagster instance migrate` |
| Postgres password | Everything (env var change) |

## Common pitfalls

### Two daemons running

`ps aux | grep dagster-daemon` shows two. Kill the duplicate
(`systemctl stop` the rogue, or `kill <pid>`). Schedules and
sensors firing twice is the symptom.

### "DAGSTER_HOME not set"

systemd units forgot the `Environment=` line. Or you started
the process from a shell that didn't export it. Every Dagster
process needs it.

### Code server starts but webserver shows "loading"

Ports / hostnames don't match. `nc -zv <host> <port>` from the
webserver host. Update `workspace.yaml`.

### `dagster dev` works, prod doesn't

`dagster dev` papers over a lot. If it works and prod doesn't:
suspect Postgres connection, env vars not propagated to systemd,
or a venv mismatch between processes.

## Related

- Config files: `skills/dagster-yaml-reference/SKILL.md`,
  `skills/workspace-yaml-reference/SKILL.md`
- Health checks: `skills/verify-deploy/SKILL.md`
- CLI quick reference: `skills/cli-cheatsheet/SKILL.md`
