"""KitStep6Rule — all kits except ``rln`` depend on the root branch's
``step6``. ``rln`` is handled by ``KitRlnRule``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import RootBranch
from ..spec.step_taxonomy import kits


@dataclass(frozen=True)
class KitStep6Rule:
    rln_exempt: bool = True

    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if step not in kits():
            return
        if self.rln_exempt and step == "rln":
            return
        yield DepEdge(
            upstream_step="step6",
            partition_rule=RootBranch(),
        )
