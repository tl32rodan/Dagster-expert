"""M2 — the entry point: spec -> dg.Definitions (whitepaper §4.4).

build_definitions(flows_dir) scans flows/*/spec.yaml, builds assets +
reconciliation sensors + jobs, and assembles one Definitions.

Local-sim: PipesSubprocessClient resource for same-node compute, and the
in_process executor (one run executes its work in-process — load-bearing
for the one-run-per-node model; at LSF scale the run worker IS the LSF
node). Launcher / storage / coordinator live in dagster.yaml, not here.
"""
from __future__ import annotations

import dagster as dg

from framework.assets.builder import build_asset
from framework.sensor.cascade import build_cascade_for_spec
from framework.sensor.factory import build_sensor
from framework.spec.loader import load_all_specs
from framework.versioning.base import resolve_version


def build_definitions(flows_dir: str) -> dg.Definitions:
    specs = load_all_specs(flows_dir)
    assets, sensors, jobs = [], [], []
    any_automation = False

    for spec in specs:
        for a in spec.assets:
            version_fn = resolve_version(spec.effective_version(a))
            assets.append(build_asset(a, spec, version_fn))
            if spec.effective_trigger(a) == "automation":
                any_automation = True

        # Sensors / jobs are only needed for compute assets in the
        # `reconciliation` trigger model. With `trigger: automation`, the
        # built-in AssetDaemon evaluates each asset's AutomationCondition.eager()
        # and cascades materializations — no reconcile sensor needed.
        for a in spec.assets:
            if a.kind == "compute" and spec.effective_trigger(a) == "reconciliation":
                job = dg.define_asset_job(
                    f"{a.name}_job",
                    selection=dg.AssetSelection.assets(a.name),
                )
                jobs.append(job)
                sensors.append(build_sensor(a, job, spec))

    # trigger=automation: framework-built cascade sensor + cascade job (one
    # of each per flow). The sensor emits one RunRequest per missing
    # (asset, partition) pair on each tick. AutomationCondition.eager() was
    # tried and rejected: AssetDaemon's evaluation of eager() against an
    # unpartitioned-entry → partitioned-downstream-with-`all`-mapping yields
    # 0 evaluations per tick in 1.13.3 (LESSONS.md L15).
    for spec in specs:
        if any(spec.effective_trigger(a) == "automation" for a in spec.assets):
            cas_jobs, cas_sensor = build_cascade_for_spec(spec)
            jobs.extend(cas_jobs)
            sensors.append(cas_sensor)

    return dg.Definitions(
        assets=assets,
        sensors=sensors,
        jobs=jobs,
        resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
        executor=dg.in_process_executor,
    )
