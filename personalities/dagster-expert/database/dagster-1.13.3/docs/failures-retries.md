# Failures, retries, run states

**Tested against Dagster 1.13.3.**

## Run states

| Final state | Meaning |
|---|---|
| `SUCCESS` | All steps materialized |
| `FAILURE` | At least one step raised; retries exhausted |
| `CANCELED` | User clicked Terminate (or daemon's run_monitoring auto-canceled) |
| `STARTED` (forever) | Worker died without writing terminal event — see `dagster-operator/diagnose-orphan-run` |

State transitions: `QUEUED → STARTING → STARTED → (SUCCESS | FAILURE | CANCELED)`.

## RetryPolicy on an asset

```python
from dagster import RetryPolicy, Backoff, Jitter, asset

@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10.0,                  # seconds between attempts
        backoff=Backoff.EXPONENTIAL, # or Backoff.LINEAR
        jitter=Jitter.PLUS_MINUS,    # randomize delay ±50%
    ),
)
def flaky_asset(): ...
```

With `max_retries=3`, asset is tried up to 4 times (1 initial + 3
retries). Each attempt logs `STEP_RETRY` events; final attempt
either `STEP_SUCCESS` or `STEP_FAILURE`.

## When to retry vs. when not to

**Retry suitable**:
- Transient network errors
- EDA tool license queue timeouts
- Filesystem hiccups
- HTTP 5xx from external service

**Retry wrong**:
- Logic errors in code (will fail forever)
- `KeyError` / `ValueError` from bad input
- Permission errors (won't get better)
- Out-of-memory (won't get better; might take down host)

For deterministic logic errors, no retry; let the run fail and
surface the bug.

## Re-execution from UI

After a `FAILURE`:

| Button | Behavior |
|---|---|
| **Re-execute** | Re-runs the entire run (every step, even ones that succeeded) |
| **Re-execute from failure** | Re-runs only failed steps + their dependents; succeeded steps are reused |

`Re-execute from failure` is what you usually want — cheaper.
Use full `Re-execute` only if you suspect a non-deterministic
upstream issue affected steps that "succeeded" before the
failing one.

`CANCELED` runs do not have re-execute-from-failure (cancel
doesn't pre-stage retry data); only full re-execute.

## `dagster.yaml` `run_monitoring` (instance-level)

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 300
  cancel_timeout_seconds: 300
  max_runtime_seconds: 86400        # 24h cap; useful for runaway ops
```

Daemon watches each STARTED run; if no heartbeat in
`start_timeout_seconds`, auto-marks `FAILURE`. Without
`run_monitoring`, orphan runs sit at STARTED forever.

This is the operator's job to set up — see
`dagster-operator/dagster-yaml-reference`.

## Common gotchas

- **`Re-execute` re-runs everything** — the button you want is
  `Re-execute from failure`.
- **Retry policy ignored** — applied inside an `@op` instead of
  `@asset`, or wrapped manually. Apply on `@asset` directly.
- **Asset shows green after a failed run** — partition that
  previously succeeded retains its OK status. Look at the run,
  not the asset color, to see what happened in this run.
- **Run stuck STARTED forever** — worker died; no terminal event
  written. Without `run_monitoring`, only manual cleanup. See
  `dagster-operator/diagnose-orphan-run`.

## Related

- Run queue, concurrency levels & why "Materialize all" looks sequential:
  [`STANDARD_USAGE.md`](STANDARD_USAGE.md) §8, §9b–c
- [`asset-basics.md`](asset-basics.md) — `@asset` decorator args
- Examples: [`07_retry_policy.py`](../examples/07_retry_policy.py)
