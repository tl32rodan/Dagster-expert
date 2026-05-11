# lab5 · failures, retries, and the event log

**Time**: 45 min · **Prerequisites**: lessons 01, 02, 03

## What you'll learn

- What an asset failure looks like in Dagster's event log
- Asset-level retry policies (`RetryPolicy`)
- The difference between FAILURE (asset failed) and CANCELED
  (run was terminated)
- Partial materialization in a partitioned asset
- "Re-execute from failure" vs "Re-execute"

## The lesson in 60 seconds

```bash
cd 05-failures
dagster dev -m flaky
# open http://127.0.0.1:3000
```

`flaky_payload` is partitioned by 4 corners. One corner — `ss_m40c`
— is rigged to raise an exception every other run.

Click "Materialize all". Three partitions go green; `ss_m40c` goes
red. Click into the failed run; the event log shows the
`Exception` and traceback.

## What you observe

In the event log of a failed materialization:

1. `EXECUTION_STEP_START` — the step began
2. `STEP_FAILURE` — exception caught, with traceback
3. `RESOURCE_TEARDOWN` — resources released
4. `RUN_FAILURE` — overall run marked FAILURE

Dagster captures this whether or not the asset has a retry policy.
Retry policy just adds the retry attempts before the final
FAILURE.

## Retry policy

```python
from dagster import RetryPolicy, Backoff, Jitter

@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10.0,                 # seconds between attempts
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def flaky_payload(...): ...
```

With this policy, the asset is tried up to 4 times (1 + 3
retries). Each attempt appears as a STEP_RETRY event in the run
log; the final attempt is STEP_SUCCESS or STEP_FAILURE.

**Use exponential backoff** for transient failures (network blip,
EDA tool license queue). Skip retries for deterministic logic
errors — retrying won't help.

## Now try

### Try 1 · Run twice

The first run fails on `ss_m40c`. Click "Re-execute" → "Re-execute
from failure". This time `ss_m40c` succeeds (the rigging only
fails on every other run). The other three partitions are NOT
re-run — Dagster knows they already succeeded.

### Try 2 · Watch retries in the event log

Edit the asset to enable the retry policy (uncomment the
`retry_policy=` line). Reload. Materialize `ss_m40c`. The event
log shows:
- STEP_START
- STEP_FAILURE (attempt 1)
- STEP_RETRY (waiting...)
- STEP_START (attempt 2)
- STEP_SUCCESS

### Try 3 · Compare FAILURE vs CANCELED

In another partition (the slow one we'll add), launch and click
"Terminate" before it finishes. The run becomes CANCELED, NOT
FAILURE. The asset materialization-store shows: no record. (FAILURE
also leaves no record by default — but the event log tells you it
was attempted.)

## Common pitfalls

- **"Re-execute" re-runs everything**: the button you want is
  "Re-execute from failure" — only re-runs failed steps.
- **Retry policy ignored**: you applied it inside an `@op` instead
  of `@asset`, or you wrapped the asset's op manually. Apply
  `retry_policy=` directly on `@asset`.
- **Asset shows green after a failed run**: a partition that
  previously succeeded retains its OK status. Look at the run, not
  the asset color, to see what happened in this run.

## Cheat sheet

```python
from dagster import asset, RetryPolicy, Backoff, Jitter

@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def my_asset(): ...
```

Run states (from lab6 cheat — preview):

| Final state | Meaning |
|---|---|
| SUCCESS | All steps materialized |
| FAILURE | At least one step raised; retries exhausted |
| CANCELED | User clicked Terminate (or daemon's run_monitoring auto-canceled) |
| STARTED (forever) | Worker died without writing terminal event — see lesson 06 |
