---
name: verify-deploy
description: 5-step health check chain after install or change. Stop at the first failure — don't keep going.
---

<!-- all-might generated -->

# verify-deploy — is the deployment healthy?

## When to use

- Right after first install
- Right after upgrading Dagster
- After changing `dagster.yaml` or `workspace.yaml`
- User reports "is dagster working?" and you don't know yet

## The 5-step chain

Run these in order. **Stop at the first failure** — later steps
depend on earlier ones, and continuing past a failure produces
confusing errors.

```bash
source /opt/dagster-venv/bin/activate            # adapt to your venv
export DAGSTER_HOME=/var/lib/dagster             # adapt to your DAGSTER_HOME
```

### Step 1 — instance config loaded

```bash
dagster instance info
```

**Pass**: prints "Storage", "Run launcher", "Run coordinator"
sections matching your `dagster.yaml`. Postgres section shows
your hostname.

**Fail signals**:
- "DAGSTER_HOME not set" → export it
- "Storage: sqlite" when you expected postgres → wrong
  `DAGSTER_HOME`, or `dagster.yaml` missing/typo
- "could not connect to server: ..." → postgres unreachable;
  `nc -zv <pg-host> 5432` to verify

### Step 2 — daemon healthy

```bash
dagster-daemon liveness-check
```

**Pass**: exits 0, no output.

**Fail signals**:
- non-zero exit → daemon isn't running, or last heartbeat too old
- check `journalctl -u dagster-daemon -n 100`
- common cause: Postgres connection lost, daemon stopped
  heartbeating

### Step 3 — webserver responding

```bash
curl -fsS http://localhost:3000/server_info
```

**Pass**: prints JSON like `{"dagit_version": "1.13.3", ...}`.

**Fail signals**:
- "Connection refused" → webserver not running
- 404 → wrong port; check `dagster-webserver` startup args
- 502 from a fronting nginx → webserver behind it died

### Step 4 — code locations load

```bash
dagster definitions validate -w /etc/dagster/workspace.yaml
```

**Pass**: prints "Validation successful for code location ..."
for each location.

**Fail signals**:
- "Could not load code location ..." → see
  `skills/diagnose-codeloc-fail/SKILL.md`
- "ModuleNotFoundError" → code server's venv missing a package
- "Error loading base asset job" → cross-location AssetSpec trap;
  see `skills/workspace-yaml-reference/SKILL.md`

### Step 5 — GraphQL reachable

```bash
dagster-graphql --remote http://localhost:3000/graphql \
    -t 'query { version }'
```

**Pass**: returns JSON `{"data": {"version": "1.13.3"}}`.

**Fail signals**:
- "ConnectionError" → webserver actually down (Step 3 lied — maybe
  /server_info returned cached)
- 401 / 403 → fronting auth blocked GraphQL endpoint; check nginx

## End-to-end smoke run

If steps 1-5 all pass, do one real materialization to prove the
run launcher + coordinator + daemon all work end-to-end:

```bash
# Pick a known-cheap asset from one of your code locations
dagster asset list -w /etc/dagster/workspace.yaml | head

# Materialize it
dagster asset materialize \
    -w /etc/dagster/workspace.yaml \
    --select <asset_key>
```

Watch in the UI: the run should appear in "Runs", go QUEUED →
STARTED → SUCCESS within a few seconds.

If the run sticks in STARTED forever → see
`skills/diagnose-orphan-run/SKILL.md`.

## Quick health snapshot for monitoring

If you need a single command to feed a health probe:

```bash
dagster-daemon liveness-check && \
    curl -fsS http://localhost:3000/server_info > /dev/null && \
    echo OK
```

Exits 0 + prints "OK" only if both daemon and webserver are healthy.

## After a Dagster version upgrade

Extra step: schema migration.

```bash
dagster instance migrate
```

Then run all 5 steps. Don't skip step 1 — it's the cheapest signal
that the migration actually applied.

## Common pitfalls

### Step 1 says SQLite but I configured Postgres

`DAGSTER_HOME` either unset or pointing somewhere with no
dagster.yaml (Dagster falls back to SQLite + that dir). Check:

```bash
echo $DAGSTER_HOME
ls -la $DAGSTER_HOME/dagster.yaml
```

### Step 2 fails right after a reboot

Daemon hasn't heartbeat-ed yet. Wait 30s and retry. If still
failing, `systemctl status dagster-daemon`.

### Step 4 loads some locations and not others

Each location is independent. The failing one's code server is
either down or has bad code. Restart it
(`systemctl restart dagster-code-<X>`) and check its journal.

### Step 5 fails but Step 3 passes

GraphQL is mounted at `/graphql`; `/server_info` is a separate
endpoint. nginx may be routing `/graphql` to the wrong upstream.
Check the nginx config.

## Related

- `skills/diagnose-orphan-run/SKILL.md` — runs stuck in STARTED
- `skills/diagnose-codeloc-fail/SKILL.md` — code location won't load
- `skills/start-services/SKILL.md` — restart procedures
