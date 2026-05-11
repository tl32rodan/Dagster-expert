---
name: diagnose-orphan-run
description: Run is stuck in STARTED forever. Find the worker, decide if it's alive, then either let run_monitoring catch it or terminate manually.
---

<!-- all-might generated -->

# diagnose-orphan-run — run stuck in STARTED

## When to use

- User says "my run shows STARTED but nothing's happening"
- A run has been STARTED for hours with no events
- `dagster run list` shows runs that should have finished long ago

## What "orphan" means here

Dagster transitions: `QUEUED → STARTING → STARTED → (SUCCESS |
FAILURE | CANCELED)`. An orphan is stuck at STARTED because:

1. The subprocess / container running it crashed without writing
   a terminal event, OR
2. The host running the worker rebooted, OR
3. Dagster's connection to the worker (e.g. Postgres event log)
   broke and the worker is dead but the DB still says STARTED.

The daemon's `run_monitoring` is meant to catch (1) and (2). Without
it, the run sits there forever.

## Step 1 — confirm it's actually orphaned

```bash
# How long has it been STARTED?
dagster run list --limit 5
```

A run that's been STARTED for under a few minutes might just be
slow. Check the asset code — is there a long-running step?

```bash
# Find the run id, then look at its events
dagster debug export <RUN_ID> /tmp/run.gz
dagster-webserver-debug /tmp/run.gz
```

Open the UI on the debug snapshot and inspect the event timeline.
Last event tells you where the worker stopped.

## Step 2 — is the worker process alive?

For `DefaultRunLauncher` (subprocess mode), the worker is a
Python process on the daemon's host:

```bash
ps -ef | grep -i dagster | grep <RUN_ID_PREFIX>
# or, more reliable:
ps -ef | grep "dagster api execute_run"
```

If you see a process with the run id in args → still alive,
maybe just slow. Don't kill it yet.

If you don't see it → it died. Move to Step 3.

For `DockerRunLauncher`:

```bash
docker ps -a | grep <RUN_ID_PREFIX>
```

Container exited / not present → dead. Container running → alive.

## Step 3 — let `run_monitoring` clean it up (preferred)

If you have `run_monitoring.enabled: true` in `dagster.yaml`:

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 300
  cancel_timeout_seconds: 300
  max_runtime_seconds: 86400
```

The daemon watches each STARTED run; if no heartbeat within
`start_timeout_seconds` (or run exceeds `max_runtime_seconds`),
it auto-marks the run FAILURE.

Wait for the daemon's next sweep (~30s) and check:

```bash
dagster run list --limit 5
```

The orphan should now show FAILURE.

## Step 4 — manual termination (when run_monitoring isn't enabled, or you can't wait)

```bash
dagster run delete <RUN_ID>
```

Wait — this **deletes the run record**, not just marks it failed.
Event log goes too. Use only if you're sure you don't need
postmortem data.

If you want to keep the record but mark it failed: there's no
clean CLI. Use GraphQL:

```bash
dagster-graphql --remote http://localhost:3000/graphql \
    -t '
mutation Terminate($runId: String!) {
  terminateRun(runId: $runId, terminatePolicy: MARK_AS_CANCELED_IMMEDIATELY) {
    __typename
  }
}
' \
    -v '{"runId": "<RUN_ID>"}'
```

This marks the run CANCELED in the DB without trying to reach a
worker (which is dead anyway).

## Step 5 — figure out why it orphaned (so it doesn't happen again)

Check daemon logs:

```bash
journalctl -u dagster-daemon --since "1 hour ago" | grep -i <RUN_ID_PREFIX>
journalctl -u dagster-daemon --since "1 hour ago" | grep -iE "error|crash"
```

Check the worker's compute logs:

```bash
ls -la $DAGSTER_HOME/compute_logs/<RUN_ID>/
cat $DAGSTER_HOME/compute_logs/<RUN_ID>/*.err
```

Common causes:
- **OOM killer**: `dmesg | grep -i "killed process"` — worker
  exceeded memory. Lower op concurrency or add more RAM.
- **Host reboot**: `last reboot` shows the box went down. Enable
  `run_monitoring` to auto-fail on next start.
- **Postgres timeout**: daemon log shows "psycopg2 ... server
  closed the connection". Postgres died or network blip. Fix the
  PG side; Dagster reconnects.
- **External tool hung**: subprocess that called an EDA tool is
  still alive but the tool blocked. `pstree -p <pid>` to see
  what the worker is waiting on.

## Step 6 — re-launch (after diagnosing)

If you want to retry the run after cleaning up:

```bash
# Re-materialize specific assets the failed run targeted
dagster asset materialize -w workspace.yaml --select <key>+
```

Or from UI: navigate to the asset and click "Materialize". For
partitioned assets, pick the same partition that failed.

## Prevention checklist

- [ ] `run_monitoring.enabled: true` in `dagster.yaml`
- [ ] `max_runtime_seconds` set to something sane (24h is fine
      for nightly batches; 1h for fast pipelines)
- [ ] Daemon is monitored externally — `dagster-daemon
      liveness-check` from a cron / nagios / whatever you use
- [ ] Postgres is reliable — orphan rate spikes when PG flakes
- [ ] Op-level memory limits or concurrency tags so one heavy
      asset can't OOM the host

## Common pitfalls

### `dagster run delete` removed all my postmortem data

It deletes the run record + event log. Always `dagster debug
export` BEFORE delete if you might want to investigate later:

```bash
dagster debug export <RUN_ID> /tmp/run-${RUN_ID}.gz
dagster run delete <RUN_ID>
# Later: dagster-webserver-debug /tmp/run-<RUN_ID>.gz
```

### The orphan reappears every time I clean it

Something is keeping the run record. Likely a re-launching
schedule/sensor firing the same job. Pause the schedule
(`dagster schedule stop -w workspace.yaml <name>`) and
investigate.

### `terminateRun` GraphQL mutation returns an error

The webserver may be on a different host than the worker; for
`MARK_AS_CANCELED_IMMEDIATELY` to work cleanly, the daemon needs
to know the worker is dead. If the daemon thinks it's alive,
this mutation may try to send a SIGTERM. Use `SAFE_TERMINATE` or
just `dagster run delete` instead.

### Nothing in compute_logs/

Worker died before writing any output, or `compute_log_manager`
isn't `LocalComputeLogManager`. Check `dagster.yaml` and
`journalctl -u dagster-daemon` for the run's STARTED event time.

## Related

- Run monitoring config: `skills/dagster-yaml-reference/SKILL.md`
- Code location load failures (different problem):
  `skills/diagnose-codeloc-fail/SKILL.md`
- CLI reference: `skills/cli-cheatsheet/SKILL.md`
