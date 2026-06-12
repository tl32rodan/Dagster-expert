"""Dispatch sensor (WHITEPAPER §3.3).

Each tick:
  for each compute asset in this flow:
    desired   = partitions_def.get_partition_keys()
    observed  = status_db.list_successful_partitions(asset)   <-- NOT Dagster materializations
    missing   = desired - observed
    for each missing partition:
      compute idempotency_key
      upsert PENDING into status DB (UNIQUE absorbs duplicates)
      emit a RunRequest (so the asset body runs and fires the bsub)

Why `observed` comes from status DB: Dagster's `get_materialized_partitions`
sees the placeholder materialization that the dispatch asset body emits
(return None → Dagster auto-emits with an auto-computed data_version).
Believing that placeholder would stop redispatch even before any real
computation completes. Status DB is execution truth (WHITEPAPER §3.3 R7).
"""
from __future__ import annotations

import dagster as dg

from framework.assets.partition_builder import build_partitions_def
from framework.fabric import status_db
from framework.spec.schema import AssetSpec, FlowSpec


def build_dispatch_sensor(
    spec: FlowSpec,
    compute_asset_specs: list[AssetSpec],
    job: dg.JobDefinition,
    db_path_resolver,
):
    """One sensor per flow, covering all compute assets.

    `db_path_resolver()` returns the absolute status DB path at runtime
    (so DAGSTER_HOME / flow-specific roots can be resolved late).
    """
    flow_name = spec.flow_name

    # Pre-compute the desired partition set per asset (immutable across ticks
    # in Phase 1 — static dimensions only).
    desired_by_asset: dict[str, list[str]] = {}
    for a in compute_asset_specs:
        pd = build_partitions_def(a.partitioned_by, spec.dimensions)
        desired_by_asset[a.name] = pd.get_partition_keys() if pd else [""]

    @dg.sensor(
        name=f"{flow_name}_dispatch_sensor",
        job=job,
        default_status=dg.DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=15,
    )
    def _dispatch_sensor(context: dg.SensorEvaluationContext):
        db_path = db_path_resolver()
        run_requests: list[dg.RunRequest] = []
        for asset_name, desired in desired_by_asset.items():
            observed = status_db.list_successful_partitions(db_path, asset_name)
            missing = [k for k in desired if k not in observed]
            for partition_key in missing:
                # The idempotency_key here is preliminary — upstream data_versions
                # are not yet known at dispatch-sensor time. The asset body
                # recomputes the final key using context inputs. We use a stable
                # run_key to dedup RunRequests across ticks while the asset body
                # is still pending or in-flight.
                run_key = f"{asset_name}|{partition_key}"
                run_requests.append(
                    dg.RunRequest(
                        run_key=run_key,
                        partition_key=partition_key,
                        tags={"fabric/asset": asset_name},
                    )
                )
        if not run_requests:
            return dg.SkipReason(
                f"all partitions observed for {flow_name}"
                f" (assets={list(desired_by_asset)})"
            )
        return run_requests

    return _dispatch_sensor
