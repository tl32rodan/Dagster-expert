"""M2 — the most error-prone module: translate a spec *intent* into a
direction-correct Dagster PartitionMapping.

Whitepaper appendix A item 1: MultiPartitionMapping direction is
dict-key=upstream, dimension_name=downstream. For the
single-dim-upstream -> multi-dim-downstream case (liberate-char's
characterize depending on pvt-only or cell-only generators), the right
primitive is MultiToSingleDimensionPartitionMapping(partition_dimension_name=<shared dim>).

build_mapping returns None when no explicit mapping is needed (Dagster's
default identity, or an unpartitioned upstream).
"""
from __future__ import annotations

from dagster import (
    AllPartitionMapping,
    LastPartitionMapping,
    MultiToSingleDimensionPartitionMapping,
    PartitionMapping,
)


def build_mapping(
    intent: str | dict[str, str],
    upstream_dims: list[str],
    downstream_dims: list[str],
) -> PartitionMapping | None:
    """Return the PartitionMapping for an (upstream -> downstream) edge.

    intent: spec mapping value ("identity"|"all"|"last"| {dim: rule})
    upstream_dims / downstream_dims: the assets' partitioned_by lists.
    """
    # Unpartitioned upstream -> no partition mapping (Dagster handles fan-in).
    if not upstream_dims:
        return None

    # dict intent => per-dimension. liberate-char uses {"pvt": "identity"} etc.
    if isinstance(intent, dict):
        # single-dim upstream feeding a multi-dim downstream along a shared dim
        if len(upstream_dims) == 1 and len(downstream_dims) >= 2:
            dim = upstream_dims[0]
            rule = intent.get(dim, "identity")
            if rule == "identity" and dim in downstream_dims:
                return MultiToSingleDimensionPartitionMapping(partition_dimension_name=dim)
            raise ValueError(
                f"unsupported dict mapping {intent!r} for {upstream_dims}->{downstream_dims}"
            )
        # same-shape multi-dim => default identity
        if upstream_dims == downstream_dims:
            return None
        raise ValueError(
            f"unsupported dict mapping {intent!r} for {upstream_dims}->{downstream_dims}"
        )

    # string intent
    if intent == "all":
        return AllPartitionMapping()
    if intent == "last":
        return LastPartitionMapping()
    if intent == "identity":
        # same shape (single- or multi-dim) => Dagster's default identity (None)
        if upstream_dims == downstream_dims:
            return None
        if (
            len(upstream_dims) == 1
            and len(downstream_dims) >= 2
            and upstream_dims[0] in downstream_dims
        ):
            return MultiToSingleDimensionPartitionMapping(
                partition_dimension_name=upstream_dims[0]
            )
    raise ValueError(
        f"cannot build mapping for intent={intent!r} {upstream_dims}->{downstream_dims}"
    )
