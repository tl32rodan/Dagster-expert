"""M2 — the entry point: spec -> dg.Definitions (WHITEPAPER §3.2).

build_definitions(flows_dir) scans flows/*/spec.yaml, builds assets +
dispatch sensor + harvest sensor (one of each per flow), and assembles
one Definitions.

Runtime path resolution is deferred to call time via small resolver
callables. This lets DAGSTER_HOME, status DB path, and flow-specific
roots be set after import (e.g., by run_demo.py or by Dagster's daemon
boot sequence).
"""
from __future__ import annotations

import os
from pathlib import Path

import dagster as dg

from framework.assets.builder import build_asset
from framework.fabric import status_db
from framework.sensor.dispatch import build_dispatch_sensor
from framework.sensor.harvest import build_harvest_sensor
from framework.spec.loader import load_all_specs
from framework.versioning.base import resolve_version


def _default_db_path() -> str:
    explicit = os.environ.get("FABRIC_STATUS_DB")
    if explicit:
        path = Path(explicit)
    else:
        home = os.environ.get("DAGSTER_HOME") or "/tmp/dagster-home"
        path = Path(home) / "fabric_status.db"
    status_db.init_db(path)
    return str(path)


def _default_log_dir() -> str:
    home = os.environ.get("DAGSTER_HOME") or "/tmp/dagster-home"
    p = Path(home) / "lsf_logs"
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def _resolve_fabric_worker(flow_name: str, flows_dir: str) -> str:
    p = Path(flows_dir) / flow_name / "fabric_worker.py"
    if not p.exists():
        raise FileNotFoundError(
            f"flow '{flow_name}' is missing fabric_worker.py at {p}. "
            f"Every flow with a compute asset must define one (WHITEPAPER §3.5)."
        )
    return str(p)


def build_definitions(
    flows_dir: str,
    *,
    invoker: list[str] | None = None,
    bsub_bin: str | None = None,
    db_path_resolver=None,
    log_dir_resolver=None,
) -> dg.Definitions:
    specs = load_all_specs(flows_dir)
    db_path_resolver = db_path_resolver or _default_db_path
    log_dir_resolver = log_dir_resolver or _default_log_dir

    def _worker_resolver(flow_name: str):
        return _resolve_fabric_worker(flow_name, flows_dir)

    assets: list = []
    jobs: list = []
    sensors: list = []

    for spec in specs:
        compute_specs = []
        for a in spec.assets:
            version_fn = resolve_version(spec.effective_version(a))
            asset = build_asset(
                a, spec, version_fn,
                db_path_resolver=db_path_resolver,
                fabric_worker_path_resolver=_worker_resolver,
                log_dir_resolver=log_dir_resolver,
                invoker=invoker,
                bsub_bin=bsub_bin or "bsub",
            )
            assets.append(asset)
            if a.kind == "compute":
                compute_specs.append(a)

        # One dispatch job per compute asset (1.13.x single-partition-shape rule).
        # The dispatch sensor's RunRequests target it.
        for a in compute_specs:
            job = dg.define_asset_job(
                name=f"{spec.flow_name}__{a.name}__dispatch_job",
                selection=dg.AssetSelection.assets(dg.AssetKey(a.name)),
            )
            jobs.append(job)
            sensors.append(
                build_dispatch_sensor(spec, [a], job=job, db_path_resolver=db_path_resolver)
            )

        # Harvest sensor needs a job for schema (never enqueued — sensor
        # always SkipReason after side effect).
        if compute_specs:
            @dg.op(name=f"{spec.flow_name}_noop_op")
            def _noop_op():
                return None

            @dg.job(name=f"{spec.flow_name}__noop_job")
            def _noop_job():
                _noop_op()

            jobs.append(_noop_job)
            sensors.append(
                build_harvest_sensor(spec.flow_name, _noop_job, db_path_resolver=db_path_resolver)
            )

    return dg.Definitions(
        assets=assets,
        jobs=jobs,
        sensors=sensors,
        executor=dg.in_process_executor,
    )
