"""SetupGateRule — every non-setup step waits on the root branch's
``step0``. Always resolves to the variant-tree root, irrespective of
which branch the downstream is.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import RootBranch
from ..spec.step_taxonomy import setup_steps


@dataclass(frozen=True)
class SetupGateRule:
    gate_step: str = "step0"

    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if step in setup_steps():
            return  # setup steps don't depend on themselves
        yield DepEdge(
            upstream_step=self.gate_step,
            partition_rule=RootBranch(),
        )
