# Postgres migration — SQLite → Postgres for Dagster 1.13.3

This document is operator-mode territory. Use when SQLite's
`SQLITE_MAX_VARIABLE_NUMBER` ceiling blocks your asset
cardinality, or when you've outgrown single-writer SQLite.

## Prerequisites

- [ ] Postgres 12+ reachable from the Dagster host (`nc -zv <pg-host> 5432`)
- [ ] A Postgres database + user with full rights:
  ```sql
  CREATE DATABASE dagster;
  CREATE USER dagster WITH PASSWORD '<long-random>';
  GRANT ALL PRIVILEGES ON DATABASE dagster TO dagster;
  ```
- [ ] `DAGSTER_HOME` already set and contains a working
  SQLite-backed `dagster.yaml`
- [ ] Decision: do you need to KEEP existing SQLite run history?
  Dagster does NOT auto-migrate. If yes, plan to either:
  - Export critical runs via `dagster debug export <RUN_ID>` first
  - Or accept that history starts fresh on Postgres

## Step 1 — install the connector

```bash
source ~/dagster-venv/bin/activate
pip install --no-index --find-links=~/wheelhouse dagster-postgres==1.13.3
# or with internet:
# pip install dagster-postgres==1.13.3
```

Confirm:
```bash
python -c "import dagster_postgres; print(dagster_postgres.__file__)"
```

## Step 2 — edit `$DAGSTER_HOME/dagster.yaml`

Add the `storage.postgres` stanza. Replace your existing
`storage:` block (if any), or add if absent.

```yaml
# $DAGSTER_HOME/dagster.yaml
storage:
  postgres:
    postgres_db:
      hostname: pg.internal              # YOUR PG host
      username: dagster
      password:
        env: DAGSTER_PG_PASSWORD         # set in service env, not here
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

telemetry:
  enabled: false                         # mandatory in air-gap
```

Set the password in your shell + (if you have systemd) the
service unit's `EnvironmentFile=`:

```bash
export DAGSTER_PG_PASSWORD='<long-random>'
```

## Step 3 — verify Postgres reachable from this host

```bash
PGPASSWORD=$DAGSTER_PG_PASSWORD psql -h pg.internal -U dagster -d dagster \
    -c "SELECT version();"
```

Should print Postgres version. If "connection refused" — check
firewall / `pg_hba.conf` / `listen_addresses`. If "password
authentication failed" — verify the password and the user's
permissions.

## Step 4 — create Dagster's schema

```bash
dagster instance migrate
```

This runs the schema migrations. Idempotent — safe to re-run.
Expected output: log lines about creating tables (event_logs,
runs, asset_keys, etc.).

Verify the schema landed:
```sql
PGPASSWORD=$DAGSTER_PG_PASSWORD psql -h pg.internal -U dagster -d dagster \
    -c "\dt"
```

Should list ~15+ tables. Includes `event_logs`, `runs`,
`asset_keys`, `asset_event_tags`, etc.

## Step 5 — verify Dagster sees Postgres

```bash
dagster instance info
```

Expected output:
```
Storage:
  Postgres at pg.internal:5432/dagster
Run launcher:
  DefaultRunLauncher
Run coordinator:
  QueuedRunCoordinator (max_concurrent_runs=10)
Compute log manager:
  LocalComputeLogManager
```

If you still see SQLite in the output, your `dagster.yaml` isn't
being read — check `$DAGSTER_HOME` env, file path, and YAML syntax.

## Step 6 — restart Dagster processes

Restart anything that holds an instance connection:
- `dagster-webserver`
- `dagster-daemon`
- Each `dagster code-server start` process (if you're on gRPC
  code locations)

```bash
# under systemd:
systemctl restart dagster-webserver dagster-daemon \
                  dagster-code-svt dagster-code-lvt dagster-code-ulvt

# manually:
pkill -f 'dagster-webserver|dagster-daemon|dagster code-server'
# then start them again
```

## Step 7 — smoke test

```bash
# 1. Launch a tiny materialization
dagster asset materialize -m pipelines --select cell_list \
    --partition 'lib_branch=svt__corner|pvtrc=tt_25'

# 2. Check it landed in Postgres
PGPASSWORD=$DAGSTER_PG_PASSWORD psql -h pg.internal -U dagster -d dagster \
    -c "SELECT run_id, status FROM runs ORDER BY create_timestamp DESC LIMIT 3;"
```

Should see your run with `SUCCESS` status. If you see rows from
old SQLite tests, double-check `dagster instance info` — you may
not actually be talking to Postgres.

## Step 8 — clean cutover (optional)

If you want to **completely** abandon SQLite:

```bash
# DESTRUCTIVE — only do this after confirming Postgres works
mv $DAGSTER_HOME/history $DAGSTER_HOME/history.sqlite-old
mv $DAGSTER_HOME/runs.db $DAGSTER_HOME/runs.db.sqlite-old 2>/dev/null
```

Leaving `.sqlite-old` files is harmless — Dagster ignores them
when `storage.postgres` is configured. But removing them ensures
no accidental fallback.

## Common gotchas

- **"FATAL: password authentication failed for user dagster"** —
  password not exported in the process env. Set
  `DAGSTER_PG_PASSWORD` in the systemd `EnvironmentFile=`, not
  just in your interactive shell.
- **"could not translate host name pg.internal"** — DNS issue.
  Test with `nc -zv` first. Use IP if DNS isn't resolving.
- **`dagster instance info` shows SQLite after edit** — your
  `dagster.yaml` isn't being read. Check `$DAGSTER_HOME` env
  var, the file path, and YAML syntax (whitespace matters).
- **`dagster instance migrate` errors with "permission denied"** —
  Postgres user doesn't have CREATE/ALTER privileges. Run:
  ```sql
  GRANT CREATE, USAGE ON SCHEMA public TO dagster;
  ```
- **Performance still slow after migration** — Postgres needs
  indices on hot columns. Dagster's migrations create them, but
  `dagster instance migrate` after an upgrade is needed to keep
  them current. Also tune `shared_buffers`, `work_mem` in
  `postgresql.conf`.

## What this does NOT solve

- **Per-partition latency** — switching to Postgres helps
  per-asset queries but doesn't compress partition-level data.
  For 1M+ partition rows, also consider per-library code
  locations (see lesson 12 README level 3).
- **Network latency** — if Postgres is on a different host /
  data center than Dagster processes, every query pays a network
  round-trip. Co-locate Dagster + Postgres when possible.
- **Concurrent writer contention** — multiple Dagster processes
  (daemon + N code servers + webserver) all writing to the
  event log can contend on inserts. Tune Postgres
  `max_connections` and use connection pooling
  (PgBouncer) if you see lock waits.

## Rollback procedure

If something goes wrong:

```bash
# 1. Stop Dagster
systemctl stop dagster-webserver dagster-daemon dagster-code-*

# 2. Revert dagster.yaml — remove the storage.postgres stanza
$EDITOR $DAGSTER_HOME/dagster.yaml

# 3. Restore SQLite files if you moved them
mv $DAGSTER_HOME/history.sqlite-old $DAGSTER_HOME/history 2>/dev/null
mv $DAGSTER_HOME/runs.db.sqlite-old $DAGSTER_HOME/runs.db 2>/dev/null

# 4. Restart
systemctl start dagster-webserver dagster-daemon dagster-code-*

# 5. Verify
dagster instance info     # should show SQLite again
```

The Postgres DB stays intact — no harm done, you can come back
to it later.

## When to next look at this

- Asset count grows past ~1M partition rows → check Postgres
  performance, consider per-library code locations
- Run history exceeds a year → may want to archive old runs out
  of `event_logs` (manual; Dagster has no built-in retention)
- Multi-team deployment → consider one Dagster instance per team
  (with its own Postgres schema or DB)
