# 6b · process gets killed mid-run (the SIGKILL case)

**Time**: 15 min

> Where 6a was a graceful cancel (SIGTERM, Python catches it),
> this lab is the ungraceful case: someone runs `kill -9` on the
> run worker, or the box reboots. The OS gives the process zero
> chance to clean up.

## Setup

```bash
cd ~/projects/dagster-lab/lab6-interrupt-rerun/6b-killed
dagster dev -m killable
# UI: http://127.0.0.1:3000
```

## Walkthrough

### Step 1 · Materialize, then find the worker process

Click **Materialize** on `killable_payload`.

In another terminal:

```bash
ps -ef | grep dagster | grep -v grep | grep -v webserver
```

You'll see a process tree:
- `dagster api grpc` — the user-code gRPC server
- `dagster api execute_run` — the worker that's running your asset

You want the `execute_run` PID. Copy it.

### Step 2 · `kill -9` the worker

```bash
kill -9 <PID>
```

That's the SIGKILL. The worker can't catch it, can't clean up.

### Step 3 · Watch the UI

Refresh the run page in the UI. What's the status?

> **Expected**: status stays `STARTED` for a while. Dagster's
> webserver doesn't immediately know the worker is dead — it
> finds out when the next heartbeat fails or when you manually
> intervene.

After ~60s (heartbeat timeout) Dagster will mark the run
`FAILURE` with a message like "Run worker stopped reporting".

### Step 4 · The asset's state

Check the asset's materializations.

> **Expected**: no new materialization. The asset function never
> reached `return MaterializeResult(...)`.

But — and this is the gotcha — **any side effects the asset's
function performed before being killed are still on disk**. If
the asset wrote a file at the start, that file is still there.
Dagster doesn't roll back side effects.

### Step 5 · Recover

Two options:

**Option A: wait for Dagster to notice (~60s)**, then "Re-execute".

**Option B: don't wait — manually mark FAILURE via CLI**:

```bash
# Identify the orphaned run
dagster run list | head -5

# Mark it failed
dagster run wipe --run-id <run_id>   # nuclear: removes the run
# OR (gentler — no CLI command for this; use UI or DB directly)
```

Then re-run via UI.

## Now try

### Try 1 · Kill the gRPC server instead of the worker

```bash
kill -9 $(pgrep -f 'dagster api grpc')
```

What does the UI say?

> **Expected**: the code location goes red in Deployment. All runs
> in that location lose their gRPC channel. The current run shows
> "RUN_FAILURE" with a transport error.
>
> dagster dev should restart the gRPC subprocess automatically —
> watch the dagster dev terminal output. You may need to manually
> reload the code location (UI button) afterward.

### Try 2 · Reboot simulation

Stop dagster dev (Ctrl-C in its terminal). Restart it.

What's the run status now?

> **Expected**: the run is still `STARTED` in the database (Dagster
> has no way to know it died — the kill happened in a different
> process tree). You'll need to manually mark it failed (sub-lab
> 6c covers this in detail).

## Common pitfalls

- **`kill -9` left orphaned files**: Dagster can't clean these up.
  Your asset code should be idempotent enough that the next run
  overwrites them safely. (See 6d for checkpoint-style patterns.)
- **Heartbeat timeout is configurable** but defaults to 60s. If
  you have very long runs and want faster failure detection, set
  `run_monitoring.poll_interval_seconds` in `dagster.yaml`. Out
  of scope here — for local dev, 60s is fine.
- **`dagster run wipe` removes the run record entirely** including
  all logs. Use carefully. For a "mark failed" without losing
  logs, the cleanest path is via the UI: a STARTED run with no
  active worker will eventually get auto-marked FAILURE; just
  wait it out.
