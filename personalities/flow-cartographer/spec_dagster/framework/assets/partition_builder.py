"""M2 — build a PartitionsDefinition from spec dimensions.

Whitepaper appendix A item 3: MultiPartitionsDefinition allows at most
one dynamic dimension. liberate-char is all-static (pvt, cell), so this
constraint is not exercised here, but dynamic dims are supported for
single-dim assets.
"""
from __future__ import annotations

from dagster import (
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    StaticPartitionsDefinition,
)

from framework.spec.schema import DimensionSpec


def _one_dim(dim: DimensionSpec, name: str) -> PartitionsDefinition:
    if dim.type == "static":
        return StaticPartitionsDefinition(dim.values)
    return DynamicPartitionsDefinition(name=dim.source or name)


def build_partitions_def(
    partitioned_by: list[str],
    dimensions: dict[str, DimensionSpec],
) -> PartitionsDefinition | None:
    if not partitioned_by:
        return None
    if len(partitioned_by) == 1:
        d = partitioned_by[0]
        return _one_dim(dimensions[d], d)
    return MultiPartitionsDefinition(
        {d: _one_dim(dimensions[d], d) for d in partitioned_by}
    )
