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
from framework.sensor.factory import build_sensor
from framework.spec.loader import load_all_specs
from framework.versioning.base import resolve_version


def build_definitions(flows_dir: str) -> dg.Definitions:
    specs = load_all_specs(flows_dir)
    assets, sensors, jobs = [], [], []

    for spec in specs:
        for a in spec.assets:
            version_fn = resolve_version(spec.effective_version(a))
            assets.append(build_asset(a, spec, version_fn))

        # one reconciliation sensor per compute asset
        for a in spec.assets:
            if a.kind == "compute":
                job = dg.define_asset_job(
                    f"{a.name}_job",
                    selection=dg.AssetSelection.assets(a.name),
                )
                jobs.append(job)
                sensors.append(build_sensor(a, job, spec))

    return dg.Definitions(
        assets=assets,
        sensors=sensors,
        jobs=jobs,
        resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
        executor=dg.in_process_executor,
    )
