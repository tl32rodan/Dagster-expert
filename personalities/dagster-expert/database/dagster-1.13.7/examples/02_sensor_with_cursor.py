"""Sensor + cursor + idempotent action.

Demonstrates the canonical "advance cursor only on success" pattern
used by the Execution Fabric's harvest sensor.

Run: dagster definitions validate -m examples.02_sensor_with_cursor
"""
import json

import dagster as dg


@dg.op
def _noop_op(): pass


@dg.job
def noop_job(): _noop_op()


@dg.sensor(
    job=noop_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def cursor_sensor(context: dg.SensorEvaluationContext):
    cursor = json.loads(context.cursor or '{"last_id": 0}')
    new_rows = _read_external(after=cursor["last_id"])
    if not new_rows:
        return dg.SkipReason("no new rows")

    processed: list[int] = []
    for row in new_rows:
        try:
            _do_side_effect(row)
            processed.append(row["id"])
        except Exception as e:
            context.log.error(f"failed at row {row['id']}: {e}")
            break    # cursor stops at last successful row

    if not processed:
        return dg.SkipReason("no progress this tick")

    return dg.SensorResult(
        run_requests=[],     # side-effect-only sensor
        cursor=json.dumps({"last_id": max(processed)}),
        skip_reason=dg.SkipReason(f"processed {len(processed)} rows"),
    )


def _read_external(after: int):
    return []   # stub — real implementation queries the truth source


def _do_side_effect(row):
    pass


defs = dg.Definitions(jobs=[noop_job], sensors=[cursor_sensor])
