"""Harvest sensor (WHITEPAPER §3.3, §7.3).

THE most fragile module. Cursor + idempotent backfill + restart safety.

Each tick:
  rows = status_db.list_unharvested_terminals(after_id=cursor, limit=200)
  for each row:
    try:
      instance.report_runless_asset_event(
        AssetMaterialization(asset_key, partition,
                             tags={'dagster/data_version': row.data_version}))
      processed.append(row.id)
    except Exception:
      break   # cursor STAYS PUT; next tick retries this row
  if processed:
    status_db.mark_harvested(processed)
    cursor = max(processed)

Correctness rules:
  1. PRE-WRITE then ADVANCE cursor — never the reverse. A crash mid-batch
     leaves cursor stale, next tick re-reads (idempotent on Dagster side).
  2. Failed report breaks the loop — partial progress is preserved (rows
     before the failure are marked harvested + cursor advances; the failed
     row + downstream stay unharvested).
  3. `report_runless_asset_event` is latest-wins on Dagster's event log;
     duplicate calls produce duplicate events but lineage reads the latest.
"""
from __future__ import annotations

import json

import dagster as dg

from framework.fabric import status_db


def build_harvest_sensor(flow_name: str, noop_job: dg.JobDefinition, db_path_resolver):
    @dg.sensor(
        name=f"{flow_name}_harvest_sensor",
        job=noop_job,
        default_status=dg.DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=15,
    )
    def _harvest_sensor(context: dg.SensorEvaluationContext):
        db_path = db_path_resolver()
        cursor = json.loads(context.cursor or '{"last_id": 0}')
        rows = status_db.list_unharvested_terminals(
            db_path, after_id=cursor.get("last_id", 0), limit=200
        )
        if not rows:
            return dg.SkipReason("no unharvested terminals")

        processed: list[int] = []
        for row in rows:
            try:
                mat = dg.AssetMaterialization(
                    asset_key=dg.AssetKey(row.asset_name),
                    partition=row.partition_key,
                    tags=(
                        {"dagster/data_version": row.data_version}
                        if row.data_version
                        else None
                    ),
                    metadata=(
                        {"lsf_job_id": row.lsf_job_id}
                        if row.lsf_job_id
                        else None
                    ),
                )
                context.instance.report_runless_asset_event(mat)
                processed.append(row.id)
            except Exception as e:
                context.log.error(
                    f"harvest failed for row {row.id} "
                    f"({row.asset_name}/{row.partition_key}): {e}"
                )
                break

        if not processed:
            return dg.SkipReason(
                f"no progress; {len(rows)} rows pending, head failed"
            )

        status_db.mark_harvested(db_path, processed)
        new_cursor = {"last_id": max(processed)}
        return dg.SensorResult(
            run_requests=[],
            cursor=json.dumps(new_cursor),
            skip_reason=dg.SkipReason(f"harvested {len(processed)} rows"),
        )

    return _harvest_sensor
