"""DepRule Protocol — each concrete rule lives under ``pipelines/rules/``.

A rule is pure: given (library, step) it emits zero-or-more DepEdges.
Per-branch logic is deferred to the edge's PartitionRule.
"""
from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable

from .dep_edge import DepEdge


@runtime_checkable
class DepRule(Protocol):
    def emit_edges(self, library: str, step: str) -> Iterable[DepEdge]: ...
