"""Calling `report_runless_asset_event` from a sensor — the harvest pattern.

The sensor reads an external store and reports materializations for an
asset that is NOT in a Definitions repository. This is exactly what
the Execution Fabric harvest sensor does (status DB SUCCESS rows →
Dagster event log materializations).

Run: dagster definitions validate -m examples.03_runless_asset_event
"""
import json

import dagster as dg

EXT_ASSET = dg.AssetKey("from_external")


@dg.op
def _noop_op(): pass


@dg.job
def noop_job(): _noop_op()


@dg.sensor(
    job=noop_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def harvest_sensor(context: dg.SensorEvaluationContext):
    cursor = json.loads(context.cursor or '{"last_id": 0}')
    new_terminals = _read_external_terminals(after=cursor["last_id"])
    if not new_terminals:
        return dg.SkipReason("no unharvested rows")

    processed = []
    for row in new_terminals:
        try:
            context.instance.report_runless_asset_event(
                dg.AssetMaterialization(
                    asset_key=EXT_ASSET,
                    partition=row["partition"],
                    tags={"dagster/data_version": row["data_version"]},
                    metadata={"source_job_id": row.get("job_id")} if row.get("job_id") else None,
                )
            )
            processed.append(row["id"])
        except Exception as e:
            context.log.error(f"harvest failed at id={row['id']}: {e}")
            break

    if not processed:
        return dg.SkipReason("no progress")

    _mark_processed(processed)
    return dg.SensorResult(
        run_requests=[],
        cursor=json.dumps({"last_id": max(processed)}),
        skip_reason=dg.SkipReason(f"harvested {len(processed)} rows"),
    )


def _read_external_terminals(after: int) -> list[dict]:
    return []   # stub — real: SQL "WHERE state='SUCCESS' AND id > after"


def _mark_processed(ids: list[int]):
    pass   # stub — real: SQL "UPDATE … SET harvested=1 WHERE id IN (…)"


defs = dg.Definitions(jobs=[noop_job], sensors=[harvest_sensor])
