"""Build all 2,100 assets (100 libraries × 21 steps).

For each (library, step) we emit one ``@asset`` whose:

- ``key_prefix=[library]`` namespaces the asset key.
- ``group_name=library`` puts every asset of one library into a single
  Dagster UI group — the lineage view collapses each library to a
  single bubble until you expand it. **This is the central design
  choice of lesson 20**: 100 groups, ~21 assets per group.
- ``partitions_def`` is one of two shared module-level objects
  (``branch_partitions`` / ``root_branch_partitions``) so
  IdentityPartitionMapping works across asset boundaries.
- ``deps=[AssetDep(...)]`` carries per-edge PartitionMapping. We never
  use ``ins=`` because that would trigger IO-manager loading; this
  lesson has no actual data flow (asset bodies are stubs).

The asset body is a stub. A real shop swaps it for a runner that
``subprocess.run`` s the per-step script — see ``demo/scale-lib/`` for
that pattern.
"""
# NOTE: do NOT `from __future__ import annotations` here. Dagster 1.13.3
# validates the asset body's ``context`` parameter annotation literally;
# with future annotations it becomes a string and Dagster rejects the
# function with ``DagsterInvalidDefinitionError``.

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    MaterializeResult,
    asset,
)

from .edges import edges_for
from .libraries import LIBRARIES
from .partitions import branch_partitions, root_branch_partitions
from .steps import is_root_only, step, step_names


def _partitions_for(step_name: str):
    return root_branch_partitions if is_root_only(step_name) else branch_partitions


def _build_asset(library: str, step_name: str):
    spec = step(step_name)
    partitions_def = _partitions_for(step_name)

    deps = [
        AssetDep(
            asset=AssetKey([library, upstream_step]),
            partition_mapping=mapping,
        )
        for upstream_step, mapping in edges_for(step_name)
    ]

    @asset(
        name=step_name,
        key_prefix=[library],
        partitions_def=partitions_def,
        group_name=library,
        deps=deps,
        compute_kind=spec.kind.value,
    )
    def _impl(context: AssetExecutionContext) -> MaterializeResult:
        branch = context.partition_key
        return MaterializeResult(
            data_version=DataVersion(f"{library}::{step_name}::{branch}::stub"),
            metadata={
                "library": library,
                "step": step_name,
                "branch": branch,
                "kind": spec.kind.value,
            },
        )

    _impl.__name__ = f"{library}__{step_name}"
    return _impl


def build_assets_for_library(library: str):
    return [_build_asset(library, s) for s in step_names()]


def build_all_assets(libraries=LIBRARIES):
    out = []
    for lib in libraries:
        out.extend(build_assets_for_library(lib))
    return out
