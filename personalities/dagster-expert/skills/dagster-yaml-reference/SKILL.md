---
name: dagster-yaml-reference
description: Minimal+production dagster.yaml templates for air-gap. Where it lives, what each section does, and what NOT to put in.
---

<!-- all-might generated -->

# dagster-yaml-reference — `$DAGSTER_HOME/dagster.yaml`

## When to use

- User asks "what goes in dagster.yaml?"
- User asks how to switch from SQLite to Postgres
- User asks about telemetry / compute logs / run launcher / coordinator
- A new air-gap deploy needs to be configured

## Where the file lives

```
$DAGSTER_HOME/dagster.yaml
```

`DAGSTER_HOME` must be set in the environment of every Dagster
process (webserver, daemon, code server, CLI). If unset, dagster
either errors or silently uses a temp dir. **Always export it
before running anything.**

```bash
export DAGSTER_HOME=/var/lib/dagster   # adjust per deploy
```

## Minimal dev `dagster.yaml`

For a single-developer setup on one machine, SQLite + local FS
is fine:

```yaml
# $DAGSTER_HOME/dagster.yaml — DEV ONLY (SQLite, single-writer)

telemetry:
  enabled: false
```

That's the whole file. Dagster fills in defaults: SQLite event
log + run storage + schedule storage in `$DAGSTER_HOME/`,
`LocalComputeLogManager` writing to `$DAGSTER_HOME/compute_logs/`,
and `DefaultRunLauncher`.

**SQLite caveat**: it's single-writer. If you run `dagster-daemon`
and `dagster-webserver` both writing concurrently, you may see
"database is locked" errors. For anything beyond solo dev, use
Postgres.

## Production `dagster.yaml` — Postgres

```yaml
# $DAGSTER_HOME/dagster.yaml — PRODUCTION

# All event logs, runs, and schedules go to one Postgres DB.
# Three storage stanzas point at the SAME db; that's correct.
storage:
  postgres:
    postgres_db:
      hostname: pg.internal       # change to your Postgres host
      username: dagster
      password:
        env: DAGSTER_PG_PASSWORD  # set in service env, not here
      db_name: dagster
      port: 5432

compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /var/lib/dagster/compute_logs

local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: /var/lib/dagster/storage

run_launcher:
  module: dagster._core.launcher
  class: DefaultRunLauncher

run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10

# Mandatory in air-gap. Outbound calls would silently fail
# anyway — but explicit > implicit.
telemetry:
  enabled: false
```

### Section-by-section

**`storage.postgres.postgres_db`** — connection params for one
Postgres database used as the event log + run storage + schedule
storage. Don't pre-create separate DBs for each; one DB is the
expected pattern.

To bootstrap the schema: `dagster instance migrate` (run once
after first install or after upgrading Dagster).

**`compute_logs.LocalComputeLogManager`** — captures stdout/stderr
of each step to local files. Path must exist and be writable by
all Dagster processes. For a multi-host deploy, point this at a
shared FS mount (NFS) or use `S3ComputeLogManager` against
self-hosted MinIO.

**`local_artifact_storage.LocalArtifactStorage`** — where the
default `FilesystemIOManager` writes asset materializations.
Same shared-FS caveat for multi-host.

**`run_launcher.DefaultRunLauncher`** — runs go as subprocesses
on the same host as the daemon. Simplest, no extra moving parts.
Other options:
- `DockerRunLauncher` (`dagster_docker.DockerRunLauncher`) — runs
  go as Docker containers locally
- `K8sRunLauncher` — **OUT OF SCOPE.** This personality refuses
  to wire that up

**`run_coordinator.QueuedRunCoordinator`** — runs land in a queue
in Postgres, daemon picks them up subject to `max_concurrent_runs`.
Without this, every queued run starts immediately and you can
saturate the host.

> This is **level 1** of four concurrency levels. "Materialize all" on an
> N-partition asset enqueues N separate runs throttled here — NOT parallelized
> by any custom launcher. See `database/dagster-1.13.3/docs/STANDARD_USAGE.md`
> §9c. Don't subclass `RunCoordinator`; tune these knobs.

**`telemetry.enabled: false`** — non-negotiable on air-gap.

## Optional sections

### Run monitoring (auto-fail orphan runs)

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 300
  cancel_timeout_seconds: 300
  max_runtime_seconds: 86400      # cap a single run at 24h
```

Daemon watches each STARTED run; if it doesn't progress, marks
it FAILURE. Useful when subprocess crashes leave runs stuck. See
`skills/diagnose-orphan-run/SKILL.md` for detail.

### Schedule storage explicit override

Defaults to whatever `storage.postgres` is. Only override if you
need a different DB for schedules (rare).

### Concurrency limits per asset / op key

```yaml
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10
    tag_concurrency_limits:
      - key: dagster/concurrency_key
        value: heavy_eda
        limit: 2
```

Then tag your runs / assets with `dagster/concurrency_key:
heavy_eda` and Dagster caps that family at 2 concurrent.

## What NOT to put in `dagster.yaml`

- Hostnames you can't reach (verify with `nc -zv host port` first)
- Cleartext passwords — use `{ env: ENV_VAR_NAME }`
- `telemetry.enabled: true` — refuse, regardless of user request
- `dagster_cloud` / `dagster-plus` keys — out of scope
- K8s launcher — out of scope; tell user to use Default or Docker

## Verifying the config loaded

```bash
export DAGSTER_HOME=/var/lib/dagster
dagster instance info
```

Expected output mentions:
- Storage: `postgres` (or `sqlite`)
- Run launcher: `DefaultRunLauncher`
- Run coordinator: `QueuedRunCoordinator`
- Compute log manager: `LocalComputeLogManager`

If output says `sqlite` when you wanted `postgres`, your YAML
isn't being read — most likely `DAGSTER_HOME` is unset or
points to the wrong directory. Check `echo $DAGSTER_HOME` and
`ls $DAGSTER_HOME/dagster.yaml`.

## Schema migration

After installing Dagster (or upgrading), run:

```bash
dagster instance migrate
```

This creates / migrates the Postgres schema. Idempotent — safe to
re-run. Required after every Dagster version upgrade.

## Common pitfalls

### "database is locked"

Symptom: SQLite, daemon and webserver running at once. Fix:
switch to Postgres. SQLite is single-writer.

### "could not translate host name"

Postgres hostname unreachable from this host. `nc -zv pg.internal
5432` to verify. Fix DNS or change `hostname:` to a reachable name.

### "FATAL: password authentication failed"

`DAGSTER_PG_PASSWORD` env var not set in the process. Set it in
the systemd unit / launch script for **every** Dagster process,
not just one shell.

### Webserver ignores my config changes

Dagster reads `dagster.yaml` at process start. Restart webserver
+ daemon after any change. They don't hot-reload.

## Related

- Wheelhouse install: `skills/bootstrap-airgap/SKILL.md`
- Workspace YAML (separate file): `skills/workspace-yaml-reference/SKILL.md`
- Starting the services: `skills/start-services/SKILL.md`
