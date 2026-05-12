"""Step7FollowRule — ``step7`` fires once ``step1`` completes for the
same branch. Modeled as a same-branch dep (SameBranch).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import SameBranch


@dataclass(frozen=True)
class Step7FollowRule:
    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if step == "step7":
            yield DepEdge(
                upstream_step="step1",
                partition_rule=SameBranch(),
            )
