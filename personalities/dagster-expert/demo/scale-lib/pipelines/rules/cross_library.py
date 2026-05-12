"""CrossLibraryRule — a (target_library, target_step) reads
(source_library, source_step) via a configurable partition_rule.

This is the only rule that emits a DepEdge with ``upstream_library!=None``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import PartitionRule


@dataclass(frozen=True)
class CrossLibraryRule:
    target_library: str
    target_step: str
    source_library: str
    source_step: str
    partition_rule: PartitionRule

    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if library == self.target_library and step == self.target_step:
            yield DepEdge(
                upstream_step=self.source_step,
                upstream_library=self.source_library,
                partition_rule=self.partition_rule,
            )
