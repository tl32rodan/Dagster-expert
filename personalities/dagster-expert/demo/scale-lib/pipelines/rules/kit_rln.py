"""KitRlnRule — ``rln`` is the early kit; it depends only on the root
branch's ``step0`` (already enforced by SetupGateRule, so this rule is
intentionally a no-op when SetupGateRule is in the registry).

We keep it as a separate, register-toggleable rule for cases where
SetupGateRule is disabled and ``rln`` still needs its setup edge.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import RootBranch


@dataclass(frozen=True)
class KitRlnRule:
    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if step == "rln":
            yield DepEdge(
                upstream_step="step0",
                partition_rule=RootBranch(),
            )
