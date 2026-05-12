"""StepChainRule — linear char-step chain (step2 -> step3 -> ... -> step6).

For step N in the chain: emit one edge to step N-1 via SameBranch
(IdentityPartitionMapping). Step 0 of the chain emits nothing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import SameBranch
from ..spec.step_taxonomy import prev_in_chain


@dataclass(frozen=True)
class StepChainRule:
    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        prev = prev_in_chain(step)
        if prev is not None:
            yield DepEdge(upstream_step=prev, partition_rule=SameBranch())
