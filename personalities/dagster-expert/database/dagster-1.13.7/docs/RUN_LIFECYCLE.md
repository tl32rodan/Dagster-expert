# RUN_LIFECYCLE.md — Dagster 1.13.7 run states + RunLauncher contract

## 1. Run state machine

```
NOT_STARTED
   │ run_request created (sensor/schedule/CLI)
   ▼
QUEUED                ← QueuedRunCoordinator holds it
   │ coordinator dequeues, calls RunLauncher.launch_run
   ▼
STARTING
   │ RunLauncher.launch_run returned; run worker starting
   ▼
STARTED              ← run worker process is alive
   │ asset/op steps run; step failures surface as STEP_FAILURE events
   ▼
SUCCESS | FAILURE | CANCELED
```

Tags (`dagster/failure_reason`, `dagster/cancel_reason`, etc.) carry
"why" for the terminal states.

## 2. RunLauncher contract (1.13.7)

Subclass `dagster._core.launcher.RunLauncher` + `ConfigurableClass`:

```python
class MyRunLauncher(RunLauncher, ConfigurableClass):
    supports_check_run_worker_health = True

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        argv = ExecuteRunArgs(
            pipeline_origin=run.job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()
        # ... spawn process / submit to scheduler ...

    def terminate(self, run_id: str) -> bool:
        # ... kill the worker; return whether terminate succeeded ...

    def check_run_worker_health(self, run) -> CheckRunHealthResult:
        # ... probe scheduler; return WorkerStatus mapping ...
        return CheckRunHealthResult(WorkerStatus.RUNNING)
```

`DefaultRunLauncher` (`dagster._core.launcher.DefaultRunLauncher`)
spawns a local subprocess — fine for self-contained orchestrator hosts.

> The Execution Fabric **uses DefaultRunLauncher** and pushes work to LSF
> via its own non-blocking client *inside the asset body* — not via a
> custom RunLauncher. See `/WHITEPAPER.md §1.3` for the rationale (daemon
> push throughput vs. >10k runs).

## 3. `QueuedRunCoordinator`

```yaml
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 64
    dequeue_use_threads: true
    dequeue_num_workers: 8
    tag_concurrency_limits:
      - key: "license/primetime"
        value: "true"
        limit: 4
```

- `max_concurrent_runs`: hard cap across all runs in this instance.
- `tag_concurrency_limits`: per-tag-value cap (license budgets, queue
  budgets, etc.). The cap is held BEFORE launch (run sits in QUEUED).
- `dequeue_num_workers`: how many concurrent `launch_run` calls. Tune
  up if launching is slow (e.g. bsub takes seconds); tune down if many
  cheap launches contend on storage.

## 4. `run_monitoring`

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 180     # NOT_STARTED→STARTED grace
  poll_interval_seconds: 60      # how often check_run_worker_health
```

Daemon process: when enabled, calls `launcher.check_run_worker_health(run)`
on STARTED runs at `poll_interval_seconds`. Returns:

```python
@dataclass
class CheckRunHealthResult:
    status: WorkerStatus          # RUNNING | SUCCESS | FAILED | UNKNOWN
    msg: str | None = None
```

`FAILED` → daemon marks the Dagster run as FAILED.

## 5. Event log

Every state transition is an event in the per-run + global event log.
Surface via:

```python
inst = dg.DagsterInstance.get()
inst.get_runs()                                                  # all runs
inst.get_run_by_id(run_id)                                       # one
inst.logs_after(run_id, cursor=-1)                               # all events
inst.get_records_for_run(run_id, of_type=dg.DagsterEventType.STEP_FAILURE)
```

For asset-keyed reads:

```python
inst.get_materialized_partitions(dg.AssetKey("x"))
inst.get_latest_data_version_record(dg.AssetKey("x"), partition="p")
inst.fetch_materializations(dg.AssetKey("x"), limit=10)
```

## 6. RunRequest from sensor / schedule

```python
return dg.RunRequest(
    run_key="stable-dedup-key",         # daemon skips dupes silently
    partition_key="2026-06-12",         # if the target asset is partitioned
    tags={"k": "v"},                    # e.g. "license/primetime": "true"
    job_name="my_job",                  # required iff sensor doesn't bind one
)
```

## 7. Gotchas

1. **`define_asset_job` cannot span mixed partition shapes.** Build one
   asset job per asset (or per shape).
2. **`SkipReason` from a sensor does NOT save the cursor.** Use
   `SensorResult(skip_reason=..., cursor=...)` to persist.
3. **Terminate is best-effort.** `RunLauncher.terminate` returning True
   doesn't guarantee the worker is dead — `run_monitoring` is the
   eventual consistency net.
4. **Run worker death between STARTED and SUCCESS shows as STARTED in
   the run table** until `run_monitoring` reaps it. The Fabric's
   reaper (Phase 2) handles its own status DB version of this.

## 8. Cross-reference

- `execution_fabric/framework/fabric/lsf_run_client.py` — the
  Fabric's non-blocking dispatcher (NOT a RunLauncher subclass)
- `execution_fabric/flows/liberate_char/fabric_worker.py` — example
  worker that runs on the LSF node
- `/WHITEPAPER.md §6` — user interrupt + rerun handling for the Fabric
