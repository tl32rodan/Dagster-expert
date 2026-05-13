"""PvtSourceRule — selected PVT-aware steps depend on the
``pvt_manifest`` observable source asset. When the PVT manifest
is re-observed and its hash changes (because Tier 2 / operator
updated the PVT spec), Dagster propagates staleness down to every
materialization of these steps.

This is the seam B Brian called out in the two-tier framing: PVT
list is a Tier-2 concern (script-internal), but its identity
needs to surface to Tier 1 so source-change propagates as
staleness. The DepEdge's ``partition_rule=None`` signals to the
translator that the upstream is unpartitioned; Dagster picks
``AllPartitionMapping`` (every partition of the downstream
observes the single source).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ..spec.dep_edge import DepEdge


# Steps that are PVT-aware in TSMC AP. Apl, pgv, cdk are the canonical
# PVT-consumers in the canonical 21-step pipeline.
_DEFAULT_PVT_STEPS = frozenset({"apl", "pgv", "cdk"})


@dataclass(frozen=True)
class PvtSourceRule:
    """Wire the ``pvt_manifest`` source asset as an upstream for any
    step in ``applies_to``. Default set covers the typical PVT-aware
    kits.
    """

    applies_to: frozenset[str] = field(default_factory=lambda: _DEFAULT_PVT_STEPS)

    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if step in self.applies_to:
            yield DepEdge(
                upstream_step="pvt_manifest",
                upstream_library=None,        # source is flat-keyed, no lib
                partition_rule=None,          # unpartitioned → Dagster default
            )
