"""DepEdge: a single upstream-edge spec at asset-definition granularity.

The PartitionRule on the edge handles per-branch resolution at runtime.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .partition_rule import PartitionRule


@dataclass(frozen=True)
class DepEdge:
    """One upstream edge for a (library, step) node.

    Attributes:
      upstream_step:    the upstream step name (logical, library-relative).
      upstream_library: ``None`` for same library; else the source library.
      partition_rule:   how to resolve upstream partition keys given a
                        downstream partition key.
    """

    upstream_step: str
    partition_rule: PartitionRule
    upstream_library: Optional[str] = None
