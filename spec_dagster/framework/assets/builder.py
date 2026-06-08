"""M2 — wrap a flow-owner script into a Dagster @asset (whitepaper §4.3).

Two kinds:
  - generator: script_fn(*partition_values) -> dict[abs_path, content].
    The framework writes each file and reports a content_hash data_version.
  - compute:   script_fn(*partition_values) -> argv list.
    The framework runs it via PipesSubprocessClient on the same node
    (local-sim) — the run worker is already where the work happens, so
    the command shells out to the (mock) tool WITHOUT bsub inside the
    asset body. (At LSF scale the bsub moves to the M4 launcher; the
    asset body is identical — see whitepaper §6.2.)
"""
# IMPORTANT: do NOT add `from __future__ import annotations` here. Dagster
# 1.13.3 validates the asset `context` annotation by resolving it to the
# AssetExecutionContext type; PEP-563 string annotations defeat that and
# raise DagsterInvalidDefinitionError. (Lesson learned — see LESSONS.md.)
from typing import Callable

import dagster as dg
from dagster import AssetExecutionContext

from framework.assets.mapping_builder import build_mapping
from framework.assets.partition_builder import build_partitions_def
from framework.spec.schema import AssetSpec, FlowSpec


def _partition_values(context, partitioned_by: list[str]):
    """Extract the partition key value(s) in partitioned_by order."""
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


def build_asset(asset_spec: AssetSpec, spec: FlowSpec, version_fn: Callable[[str], str]):
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
            from pathlib import Path

            blob = ""
            for path in sorted(files):
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_text(files[path])
                blob += files[path]
            return dg.MaterializeResult(
                data_version=dg.DataVersion(version_fn(blob)),
                metadata={"files": len(files), "partition": "/".join(map(str, vals)) or "-"},
            )
        return _generator

    if asset_spec.kind == "compute":
        @dg.asset(**common)
        def _compute(
            context: AssetExecutionContext,
            pipes_subprocess_client: dg.PipesSubprocessClient,
        ) -> dg.MaterializeResult:
            vals = _partition_values(context, asset_spec.partitioned_by)
            argv = script_fn(*vals)  # full command; NO bsub inside (local-sim)
            return pipes_subprocess_client.run(
                command=argv, context=context
            ).get_materialize_result()
        return _compute

    raise ValueError(f"unknown kind {asset_spec.kind!r}")


def _import(ref: str):
    from framework.spec.loader import import_callable

    return import_callable(ref)
