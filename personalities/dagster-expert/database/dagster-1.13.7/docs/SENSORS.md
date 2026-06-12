# SENSORS.md — Dagster 1.13.7 @sensor + cursor + runless events

## 1. Anatomy of a sensor

```python
import dagster as dg

@dg.sensor(
    job=my_job,                          # required (schema); can be a no-op
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def my_sensor(context: dg.SensorEvaluationContext):
    # 1. Read state — typically from cursor or an external store
    # 2. Decide: emit RunRequest(s), SkipReason, or write side effects
    # 3. Return RunRequest / list[RunRequest] / SensorResult / SkipReason
    ...
```

Keys:
- **`job=`** is required by the schema even when the sensor never emits
  RunRequests (e.g. harvest sensor with only side effects). Pass a
  trivial `@dg.job` containing a single no-op op.
- **`default_status=RUNNING`** makes the sensor tick without UI
  interaction (needed for headless daemon deploys).
- **`minimum_interval_seconds`** floors the tick rate.

## 2. The cursor pattern (state across ticks)

```python
import json

@dg.sensor(job=noop, default_status=dg.DefaultSensorStatus.RUNNING)
def cursor_sensor(context):
    cursor = json.loads(context.cursor or '{"last_id": 0}')
    new_rows = read_external(after=cursor["last_id"])
    if not new_rows:
        return dg.SkipReason("no new rows")
    # ... process new_rows ...
    new_last = max(r.id for r in new_rows)
    return dg.SensorResult(
        run_requests=[...],                # or empty for side-effect-only
        cursor=json.dumps({"last_id": new_last}),
        skip_reason=dg.SkipReason("...") if not new_rows else None,
    )
```

Rules:
- **Cursor is a plain string** — encode/decode JSON yourself.
- **Cursor advances ONLY on successful processing.** If the side effect
  fails mid-batch, leave cursor where it was (or advance to the prefix's
  last successful row). Next tick re-reads the un-processed suffix.
- **Cursor survives daemon restart.** That's the whole point.

## 3. `report_runless_asset_event` — side effect from a sensor

```python
context.instance.report_runless_asset_event(
    dg.AssetMaterialization(
        asset_key=dg.AssetKey("my_asset"),
        partition="2026-06-12",          # optional; nullable for unpartitioned
        tags={"dagster/data_version": "v1"},
        metadata={"rows": 100},
    )
)
```

Semantics in 1.13.7:
- The event is **appended** to the global event log (not upserted).
- `get_materialized_partitions(asset_key)` will include the partition.
- `get_latest_data_version_record(asset_key, partition=...)` reads the
  most-recent matching event — **latest-wins**.
- **A sensor CAN call this method** and then `return SkipReason` — the
  side effect is preserved.
- **Duplicate calls** produce duplicate events; staleness uses the
  latest. This is **idempotent enough** for at-least-once delivery —
  exactly what the Execution Fabric harvest sensor needs.

## 4. RunRequest dedup

`RunRequest(run_key="k", partition_key="p")`:
- Dagster's sensor daemon **dedups by `run_key`**: if a run with this
  key was already launched, the request is skipped silently. The daemon
  log shows `Skipping N runs already completed with run keys: [...]`.
- A stable `run_key` across ticks is the standard pattern for
  desired-minus-observed sensors.

## 5. Schedule vs sensor

| | Schedule | Sensor |
|---|---|---|
| Trigger | Cron expression | Tick + custom logic |
| State | Stateless (current time) | Cursor (any string) |
| Use for | "Every Monday at 9am" | "When external thing X happens" |

Sensors are the workhorse for the Execution Fabric (dispatch + harvest);
schedules are not used.

## 6. Gotchas seen in 1.13.7

1. **`@dg.sensor` without `job=` is rejected.** Provide a no-op job
   even for side-effect-only sensors.
2. **`SensorResult.run_requests=[]` + `skip_reason=SkipReason(...)`** is
   the correct shape for "we did work via side effects but emit no
   runs". Just returning `SkipReason` discards the cursor update.
3. **Cursor is per-sensor**, not per-asset. If you need per-asset
   cursors, encode them inside the JSON.
4. **`context.instance` is the production `DagsterInstance`** — calls
   on it persist immediately. There's no transaction around a tick;
   side effects survive even if Python raises after the call.

## 7. Cross-reference

- Harvest sensor implementation in this codebase:
  `execution_fabric/framework/sensor/harvest.py`
- Dispatch sensor: `execution_fabric/framework/sensor/dispatch.py`
- Tests covering cursor + idempotency + partial failure + restart:
  `execution_fabric/tests/test_harvest_sensor.py`
