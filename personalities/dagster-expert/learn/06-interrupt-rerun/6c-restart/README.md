# 6c · `dagster dev` restart with in-flight runs

**Time**: 15 min

> The case 6b touched but didn't fully drill: you Ctrl-C `dagster dev`
> while a run is mid-execution, then bring it back up. What state
> is the world in?

## Setup

```bash
cd ~/projects/dagster-lab/lab6-interrupt-rerun/6c-restart
dagster dev -m chunked
# UI: http://127.0.0.1:3000
```

## Walkthrough

### Step 1 · Materialize, then Ctrl-C `dagster dev`

Click **Materialize** on `chunked_payload`. The run launches; it
will take ~30s.

While it's running, **Ctrl-C** the `dagster dev` terminal. All
Dagster processes (webserver, daemon, gRPC, worker) die.

### Step 2 · Look around the filesystem

```bash
ls /tmp/dagster-lab-6c/
# You'll see partial chunk files written before the kill
```

Side effects from the dead worker remain. Dagster can't clean
them — it didn't survive.

### Step 3 · Restart dagster dev

```bash
dagster dev -m chunked
```

Open the UI again. Look at the **Runs** tab.

> **Expected**: the run from before is still listed with status
> `STARTED`. Dagster has no way to know the worker died — the kill
> happened in processes outside Dagster's bookkeeping. Without
> intervention, this run is **orphaned** — listed as live but
> nothing is actually running.

### Step 4 · The auto-recovery (or lack thereof)

Wait ~60 seconds. Refresh the run page.

> Some Dagster versions auto-mark orphaned runs `FAILURE` after a
> heartbeat timeout (the `run_monitoring` setting in
> `dagster.yaml`). Dagster 1.13.3's default is to NOT auto-recover
> in dev mode — it's intentional, so you can debug without things
> being marked failed underneath you.

So the run sits at `STARTED` forever unless you act.

### Step 5 · Manual cleanup

Two options.

**Option A — wipe** (simple, loses logs):

```bash
dagster run list | head -3
dagster run wipe --run-id <run_id>
```

**Option B — re-run on top** (safer): just click **Materialize**
again. The new run executes in parallel with the orphaned one (no
real conflict because the orphan isn't actually running). After
the new run succeeds, the asset is materialized; the orphan record
remains as historical noise.

Option B is what most teams do in practice. The orphan's logs are
still a useful forensic record.

### Step 6 · Look at the asset's materialization history

The asset's history shows only the SUCCESSFUL run's
materialization. The orphaned run never wrote one (it died before
returning).

## Now try

### Try 1 · Configure run_monitoring for auto-recovery

Create a `dagster.yaml` next to your code:

```yaml
# dagster.yaml
run_monitoring:
  enabled: true
  poll_interval_seconds: 30
  max_runtime_seconds: 600     # mark FAILURE if a run runs >10min
```

Set `DAGSTER_HOME` to the dir containing this file:

```bash
export DAGSTER_HOME=$PWD
dagster dev -m chunked
```

Now repeat the kill-and-restart. After ~60s the orphaned run gets
auto-marked `FAILURE`.

> Trade-off: in dev you might not want this (a long debugging
> sleep gets killed). In production, you do.

### Try 2 · Restart while many runs in flight

Launch 5 runs (use the partitioned variant in 6c — there's a
`pvts` partitions def with TT, FF, SS, etc). Ctrl-C dagster dev.
Restart.

> All 5 sit `STARTED` orphaned. You can wipe them en masse with
> `dagster run wipe --run-id <id>` per run, or use the UI's
> bulk-select.

## What dagster dev actually does on restart

1. Re-reads `dagster.yaml` and the workspace
2. Spawns fresh gRPC servers per code location
3. Starts the daemon (which runs schedules + sensors + run-launcher)
4. Starts the webserver
5. **Does NOT touch existing run records** — the run-storage
   sqlite database persists across restarts. Whatever state was
   there, that's what you see.

This is why orphaned runs stay orphaned: Dagster has no
"liveness check on startup" by default. Your job to clean up.

## Common pitfalls

- **`DAGSTER_HOME` not set, you lost runs**: dagster dev defaults
  to a tempdir if `DAGSTER_HOME` isn't set. Restarts don't share
  state. **Always set `DAGSTER_HOME`** for any non-trivial work,
  even local dev.
- **"Reload" doesn't fix orphans**: the reload button in the UI
  reloads code locations, NOT run state. Orphans need wipe or
  monitoring.
- **`dagster run wipe` is destructive**: it removes the run
  record AND all event logs from the database. Use only on
  orphans you're sure are dead.
