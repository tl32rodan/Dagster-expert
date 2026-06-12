# 1_13_7_RELEASE_NOTES.md — what changed in 1.13.7 that matters here

> Source: `github.com/dagster-io/dagster/blob/1.13.7/CHANGES.md` (1.13.4
> through 1.13.7). Filtered for items that could break the Execution
> Fabric framework or the air-gap deploy.

## TL;DR — what's load-bearing

1. **`DagsterInstance.report_runless_asset_event` is stable.** Same API
   as 1.13.3+; sensor side-effect path used by the harvest sensor works
   unchanged. Verified by the framework's 52-test suite + end-to-end
   demo.
2. **`MultiToSingleDimensionPartitionMapping` is still beta.** `BetaWarning`
   is emitted but the class behaves as in 1.13.3+. We ignore the
   warning.
3. **`@dg.asset` body returning `None`** still auto-emits a placeholder
   `ASSET_MATERIALIZATION` event with auto-computed `data_version`.
   This is the foundation of the dispatch-asset-body pattern; if it
   ever changes, the dispatch sensor's `observed = status DB` rule
   becomes more, not less, important.
4. **`define_asset_job` still rejects mixed partition shapes** in one
   selection. The generator builds one job per compute asset.

## Items confirmed unchanged from 1.13.3

- `RunLauncher` ABC signature (`launch_run`, `terminate`,
  `check_run_worker_health`, `supports_check_run_worker_health`)
- `QueuedRunCoordinator` config keys (`max_concurrent_runs`,
  `tag_concurrency_limits`, `dequeue_use_threads`, `dequeue_num_workers`)
- `@dg.sensor(job=..., default_status=..., minimum_interval_seconds=...)`
- `SensorEvaluationContext.cursor` (plain string; JSON-encode yourself)
- `SensorResult(run_requests=..., cursor=..., skip_reason=...)`
- `instance.get_materialized_partitions`, `get_latest_data_version_record`,
  `logs_after`, `get_runs`, `get_records_for_run`

## Items we tested explicitly in 1.13.7

| Test | File | Result |
|---|---|---|
| Sensor body calls `report_runless_asset_event`, returns `SkipReason` — side effect persists | `execution_fabric/tests/test_harvest_sensor.py` | PASS (10 tests) |
| Sensor cursor advances ONLY on success path; partial-failure preserves prefix | same | PASS |
| `MultiToSingleDimensionPartitionMapping` resolves correctly for multi→single dim mappings | `execution_fabric/tests/test_mapping_builder.py` | PASS (with BetaWarning) |
| `define_asset_job` with single-asset selection succeeds for multi-partitioned asset | `execution_fabric/framework/generator.py` (runtime) | PASS |
| `DefaultRunLauncher` + `QueuedRunCoordinator` in `dagster.fabric.yaml` | `scripts/run_demo.py` | PASS (9/9 partitions, 5 min end-to-end) |

## Air-gap caveats unchanged

All AIRGAP_DELTAS.md items still apply. 1.13.7 didn't change
`telemetry: { enabled: false }`, the lack of need for `dg`/`uv`, or any
PostgreSQL behavior.

## If you're looking for…

| Looking for | Status in 1.13.7 |
|---|---|
| Components / `dagster_components` | Still `dg`-only; not used here |
| `AutomationCondition.eager()` | Exists; we don't use it (caused 0-eval per tick at multi-partition cross-product scale; Fabric uses its own sensors instead) |
| `AssetCheck` | Exists; not used by the Fabric framework |
| Pipes (`PipesSubprocessClient`) | Exists; not used by the Fabric framework (worker writes status DB directly) |
| `report_runless_asset_event` for FAILED events | Yes — `AssetFailedToMaterialize` is the dual; the Fabric currently emits SUCCESS-only and lets dispatch sensor leave FAILED rows un-redispatched until a manual rerun changes upstream data_version |
