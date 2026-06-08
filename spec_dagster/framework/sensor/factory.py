"""M3 — build a reconciliation sensor for a compute asset (whitepaper §5.3).

desired = every partition key of the compute asset
observed = materialized partition keys (event log)
emit one RunRequest per missing key (run_key stable via the key itself).

For liberate-char there is no work_items batching, so each leaf is its
own run — matching the hand-rolled converted/ reference. The cursor
carries nothing load-bearing (state lives in the event log); we store a
fingerprint for observability only.
"""
from __future__ import annotations

import dagster as dg

from framework.assets.partition_builder import build_partitions_def
from framework.sensor.planner import plan_reconcile
from framework.sensor.reconcile import observed_partitions
from framework.spec.schema import AssetSpec, FlowSpec


def build_sensor(asset_spec: AssetSpec, job: dg.JobDefinition, spec: FlowSpec):
    partitions_def = build_partitions_def(asset_spec.partitioned_by, spec.dimensions)
    desired_keys = set(partitions_def.get_partition_keys())
    asset_name = asset_spec.name

    @dg.sensor(
        name=f"{asset_name}_reconcile_sensor",
        job=job,
        minimum_interval_seconds=30,
        default_status=dg.DefaultSensorStatus.RUNNING,  # daemon runs it without manual enable
    )
    def _sensor(context: dg.SensorEvaluationContext):
        observed = observed_partitions(context.instance, asset_name)
        missing = plan_reconcile(desired_keys, observed)
        if not missing:
            return dg.SkipReason(
                f"{asset_name}: all {len(desired_keys)} partitions materialized"
            )
        requests = [
            dg.RunRequest(run_key=f"{asset_name}:{key}", partition_key=key)
            for key in missing
        ]
        context.log.info(
            f"{asset_name}: {len(missing)} missing of {len(desired_keys)} "
            f"-> {len(requests)} RunRequests"
        )
        return dg.SensorResult(run_requests=requests)

    return _sensor
