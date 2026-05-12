"""ParentMirrorRule — at specified steps, non-root branches additionally
read the matching upstream-step output from their variant-tree parent.

For step S in ``applies_to``:
  edge: (upstream_step=S_prev, ParentOfDownstream(include_self=False))

where ``S_prev`` is the previous chain step. The PartitionRule resolves
to ``{parent_of(downstream)}`` — i.e. a non-root branch's step5 reads
its parent's step4 in addition to its own step4 (which the StepChainRule
already emits with SameBranch). The registry merges the two SameBranch
+ ParentOfDownstream edges with the same upstream into a single
UnionOf when needed.

Configurable: ``applies_to`` is the set of downstream chain steps where
the mirror fires. Default is empty; users override per workload.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ..spec.dep_edge import DepEdge
from ..spec.partition_rule import ParentOfDownstream
from ..spec.step_taxonomy import prev_in_chain


@dataclass(frozen=True)
class ParentMirrorRule:
    applies_to: frozenset[str] = field(default_factory=frozenset)
    """Set of *downstream* chain steps where the parent-mirror edge fires."""

    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]:
        if step not in self.applies_to:
            return
        prev = prev_in_chain(step)
        if prev is None:
            return
        yield DepEdge(
            upstream_step=prev,
            partition_rule=ParentOfDownstream(
                include_self=False,
                to_root=False,
            ),
        )
