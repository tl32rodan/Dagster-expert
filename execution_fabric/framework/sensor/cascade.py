"""Framework-built cascade sensor.

For `trigger: automation` (default), the framework attaches:
  - one job per non-entry asset (so each job has a single partition shape;
    1.13.3 `define_asset_job` rejects a selection spanning multiple
    partition definitions — see LESSONS.md L16);
  - one per-flow cascade sensor that, on each tick, computes
    `desired − observed` for every asset and emits one RunRequest per
    missing (asset, partition), targeting that asset's job.

Why not `AutomationCondition.eager()`: in 1.13.3, AssetDaemon's eager
evaluation against an unpartitioned-entry → partitioned-downstream
(mapping `all`) yields 0 evaluations per tick (LESSONS.md L15). The
framework-built sensor is simpler and proven via D1 reconcile-sensor
evidence.

The sensor uses `default_status=RUNNING` (L6) so a headless daemon
evaluates it without UI interaction. `minimum_interval_seconds=10` is
demo-friendly; tune up at LSF scale per whitepaper §5.3.
"""
from __future__ import annotations

import dagster as dg

from framework.assets.partition_builder import build_partitions_def
from framework.spec.schema import FlowSpec


def _has_any_materialization(instance, key: dg.AssetKey) -> bool:
    recs = instance.get_event_records(
        event_records_filter=dg.EventRecordsFilter(
            event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=key,
        ),
        limit=1,
    )
    return bool(recs)


def _build_cascade_sensor(spec: FlowSpec, asset_to_job: dict[str, dg.JobDefinition]):
    @dg.sensor(
        name=f"{spec.flow_name.replace('-', '_')}_cascade_sensor",
        jobs=list(asset_to_job.values()),
        minimum_interval_seconds=10,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def _cas(context: dg.SensorEvaluationContext):
        requests = []
        for a in spec.assets:
            if a.kind == "entry":
                continue
            key = dg.AssetKey(a.name)
            if a.partitioned_by:
                pd = build_partitions_def(a.partitioned_by, spec.dimensions)
                desired = set(pd.get_partition_keys())
                observed = set(context.instance.get_materialized_partitions(key))
                missing = sorted(desired - observed)
                for pk in missing:
                    requests.append(dg.RunRequest(
                        run_key=f"{a.name}:{pk}",
                        job_name=asset_to_job[a.name].name,
                        partition_key=pk,
                    ))
            else:
                if not _has_any_materialization(context.instance, key):
                    requests.append(dg.RunRequest(
                        run_key=f"{a.name}:-",
                        job_name=asset_to_job[a.name].name,
                    ))

        if not requests:
            return dg.SkipReason(
                f"{spec.flow_name}: all desired (asset × partition) pairs materialized"
            )
        context.log.info(
            f"{spec.flow_name} cascade: emitting {len(requests)} RunRequest(s)"
        )
        return dg.SensorResult(run_requests=requests)

    return _cas


def build_cascade_for_spec(spec: FlowSpec):
    """Build per-asset cascade jobs + one cascade sensor for a flow."""
    asset_to_job: dict[str, dg.JobDefinition] = {}
    for a in spec.assets:
        if a.kind == "entry":
            continue
        asset_to_job[a.name] = dg.define_asset_job(
            name=f"{spec.flow_name.replace('-', '_')}__{a.name}__cas_job",
            selection=dg.AssetSelection.assets(a.name),
        )
    sensor = _build_cascade_sensor(spec, asset_to_job)
    return list(asset_to_job.values()), sensor
