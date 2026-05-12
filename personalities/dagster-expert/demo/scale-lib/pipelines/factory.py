"""Layer 4 — the only place that constructs ``@asset`` decorations.

For each (library, step) the factory:

1. asks the registry for upstream edges,
2. translates each edge into an ``AssetIn`` (Layer 3),
3. picks a ``PartitionsDefinition`` from the step's ``StepKind``,
4. emits an ``@asset`` whose body calls the appropriate runner.

This file is mechanical; no business logic. All dep policy lives in
``registry.py`` and ``rules/``.
"""
# NOTE: no `from __future__ import annotations` here — Dagster validates
# the context parameter's annotation literally; with future annotations
# the type becomes a string and Dagster rejects it.

from pathlib import Path
from typing import Optional

from dagster import (
    AssetExecutionContext,
    DataVersion,
    MaterializeResult,
    asset,
)

from .partitions import branch_partitions, root_branch_partitions
from .registry import DEPS
from .runners import PerlRunner, PythonRunner
from .source_observers import SOURCE_ASSETS
from .spec.step_taxonomy import Runner, StepKind, step, step_names
from .translator import to_asset_dep


def _partitions_for(kind: StepKind):
    if kind in (StepKind.SETUP_ROOT_ONLY, StepKind.KIT_ROOT_ONLY):
        return root_branch_partitions
    return branch_partitions


def _runner_resource_key(spec_runner: Runner) -> str:
    return "perl_runner" if spec_runner is Runner.PERL else "python_runner"


def _build_asset(library: str, step_name: str):
    spec = step(step_name)
    partitions_def = _partitions_for(spec.kind)
    runner_key = _runner_resource_key(spec.runner)

    downstream_keys = frozenset(partitions_def.get_partition_keys())
    deps = [
        to_asset_dep(library, edge, downstream_keys=downstream_keys)
        for edge in DEPS.edges_for(library, step_name)
    ]

    # group_name: per-library, per-step-kind. Used by Dagster UI to
    # color and bucket assets.
    group_name = f"{library}__{spec.kind.value}"

    @asset(
        name=step_name,
        key_prefix=[library],
        partitions_def=partitions_def,
        group_name=group_name,
        deps=deps,
        required_resource_keys={runner_key},
        compute_kind=spec.runner.value,
    )
    def _impl(context: AssetExecutionContext) -> MaterializeResult:
        # Resolve branch via partition key (single dim).
        branch = context.partition_key
        # Pick the right runner from the resource graph.
        runner = (
            context.resources.perl_runner
            if spec.runner is Runner.PERL
            else context.resources.python_runner
        )
        summary = runner.run(
            context=context,
            library=library,
            branch=branch,
            step=step_name,
        )
        return MaterializeResult(
            data_version=DataVersion(summary.data_version),
            metadata={
                "library": library,
                "step": step_name,
                "branch": branch,
                "file_count": summary.file_count,
                "total_bytes": summary.total_bytes,
                "latest_mtime": summary.latest_mtime,
                "runner": spec.runner.value,
            },
        )

    _impl.__name__ = f"{library}__{step_name}"
    return _impl


def build_library_assets(library: str):
    return [_build_asset(library, name) for name in step_names()]


def build_all_assets(libraries: list[str]):
    out = list(SOURCE_ASSETS)
    for lib in libraries:
        out.extend(build_library_assets(lib))
    return out
