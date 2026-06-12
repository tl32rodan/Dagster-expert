"""M2 — wrap a flow-owner script into a Dagster @asset (WHITEPAPER §3.2).

Three kinds:
  - entry:     trivial root; emits an empty MaterializeResult.
  - generator: script_fn(*partition_values) -> dict[abs_path, content].
               The framework writes each file and reports a content_hash
               data_version. Runs in-process (cheap).
  - compute:   script_fn(*partition_values) -> argv list (inner command, NO bsub).
               The asset body computes idempotency_key, calls
               lsf_run_client.dispatch (non-blocking), and returns None.
               Dagster auto-emits a placeholder materialization; the real
               one comes from the harvest sensor after fabric_worker writes
               SUCCESS to status DB. See WHITEPAPER §3.3 (R7 placeholder
               hazard) and §3.4 (lsf_run_client contract).
"""
# IMPORTANT: do NOT add `from __future__ import annotations` here. Dagster
# 1.13.x validates the asset `context` annotation by resolving it to the
# AssetExecutionContext type; PEP-563 string annotations defeat that and
# raise DagsterInvalidDefinitionError.
from pathlib import Path
from typing import Callable

import dagster as dg
from dagster import AssetExecutionContext

from framework.assets.mapping_builder import build_mapping
from framework.assets.partition_builder import build_partitions_def
from framework.fabric import lsf_run_client, status_db
from framework.spec.schema import AssetSpec, FlowSpec


def _partition_values(context, partitioned_by: list[str]):
    if not partitioned_by:
        return []
    if len(partitioned_by) == 1:
        return [context.partition_key]
    kd = context.partition_key.keys_by_dimension
    return [kd[d] for d in partitioned_by]


def _build_deps(asset_spec: AssetSpec, spec: FlowSpec) -> list[dg.AssetDep]:
    dims_by_asset = {a.name: a.partitioned_by for a in spec.assets}
    deps = []
    for dep in asset_spec.depends_on:
        up_dims = dims_by_asset[dep.asset]
        mapping = build_mapping(dep.mapping, up_dims, asset_spec.partitioned_by)
        if mapping is None:
            deps.append(dg.AssetDep(dg.AssetKey(dep.asset)))
        else:
            deps.append(dg.AssetDep(dg.AssetKey(dep.asset), partition_mapping=mapping))
    return deps


def _collect_upstream_data_versions(context, depends_on) -> list[str]:
    """For idempotency_key: sorted list of upstream partitions' data_versions.

    Dagster surfaces them via context.asset_partitions_def_for_input + the
    input's loaded record. In 1.13.x we use the simpler indirect path of
    the resolved partition mappings; if the run hasn't been given inputs
    (placeholder dispatch path), fall back to an empty list — the
    idempotency_key still uniquely identifies (asset, partition), good
    enough for §6.1.
    """
    versions: list[str] = []
    try:
        for inp in context.op_def.input_defs:
            try:
                rec = context.instance.get_latest_data_version_record(
                    dg.AssetKey(inp.name)
                )
                if rec and rec.data_version:
                    versions.append(rec.data_version.value)
            except Exception:
                pass
    except Exception:
        pass
    return versions


def build_asset(
    asset_spec: AssetSpec,
    spec: FlowSpec,
    version_fn: Callable[[str], str],
    *,
    db_path_resolver: Callable[[], str] | None = None,
    fabric_worker_path_resolver: Callable[[str], str] | None = None,
    log_dir_resolver: Callable[[], str] | None = None,
    invoker: list[str] | None = None,
    bsub_bin: str = "bsub",
):
    """Build a @asset for one AssetSpec.

    Compute assets need resolvers for runtime paths (status DB, worker
    script, log dir) and an optional invoker (for tests). Resolvers are
    called at asset-body time, not definition time — so DAGSTER_HOME and
    flow-specific env vars can be set after import.
    """
    partitions_def = build_partitions_def(asset_spec.partitioned_by, spec.dimensions)
    deps = _build_deps(asset_spec, spec)
    script_fn = _import(asset_spec.script) if asset_spec.script else None

    common = dict(
        name=asset_spec.name,
        partitions_def=partitions_def,
        deps=deps,
        op_tags=asset_spec.op_tags or None,
        group_name=("characterize" if asset_spec.kind == "compute" else "sources"),
    )

    if asset_spec.kind == "entry":
        @dg.asset(**common)
        def _entry() -> dg.MaterializeResult:
            return dg.MaterializeResult()
        return _entry

    if asset_spec.kind == "generator":
        @dg.asset(**common)
        def _generator(context: AssetExecutionContext) -> dg.MaterializeResult:
            vals = _partition_values(context, asset_spec.partitioned_by)
            files: dict[str, str] = script_fn(*vals)
            blob = ""
            for path in sorted(files):
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_text(files[path])
                blob += files[path]
            return dg.MaterializeResult(
                data_version=dg.DataVersion(version_fn(blob)),
                metadata={
                    "files": len(files),
                    "partition": "/".join(map(str, vals)) or "-",
                },
            )
        return _generator

    if asset_spec.kind == "compute":
        if db_path_resolver is None or fabric_worker_path_resolver is None:
            raise ValueError(
                f"compute asset '{asset_spec.name}' requires db_path_resolver "
                "and fabric_worker_path_resolver (see build_definitions)"
            )
        lsf_res = spec.effective_lsf(asset_spec)
        lsf_cfg = lsf_run_client.LSFConfig(
            queue=lsf_res.queue,
            cores=lsf_res.cores,
            mem_mb=lsf_res.mem_mb,
            walltime=lsf_res.walltime,
            project=lsf_res.project,
        )

        @dg.asset(**common)
        def _compute(context: AssetExecutionContext):
            vals = _partition_values(context, asset_spec.partitioned_by)
            inner_argv = script_fn(*vals)
            upstream_dvs = _collect_upstream_data_versions(context, asset_spec.depends_on)
            idem_key = status_db.compute_idempotency_key(
                asset_spec.name, context.partition_key, upstream_dvs
            )
            db_path = db_path_resolver()
            fabric_worker_path = fabric_worker_path_resolver(spec.flow_name)
            log_dir = (log_dir_resolver or (lambda: "/tmp/lsf_logs"))()
            lsf_run_client.dispatch(
                idempotency_key=idem_key,
                asset_name=asset_spec.name,
                partition_key=context.partition_key,
                inner_argv=list(inner_argv),
                fabric_worker_path=fabric_worker_path,
                db_path=db_path,
                lsf_cfg=lsf_cfg,
                log_dir=log_dir,
                bsub_bin=bsub_bin,
                invoker=invoker,
            )
            context.log.info(
                f"dispatched {asset_spec.name}/{context.partition_key} "
                f"idem_key={idem_key[:12]}…"
            )
            # No MaterializeResult: harvest sensor produces it.
            # Dagster auto-emits a placeholder; status DB is execution truth.
            return None
        return _compute

    raise ValueError(f"unknown kind {asset_spec.kind!r}")


def _import(ref: str):
    from framework.spec.loader import import_callable
    return import_callable(ref)
