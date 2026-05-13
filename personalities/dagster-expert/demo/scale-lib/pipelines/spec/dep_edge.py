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
                        downstream partition key. ``None`` means the
                        upstream is unpartitioned (e.g. an
                        ``observable_source_asset``); the translator
                        will omit ``partition_mapping`` on the AssetDep
                        and let Dagster apply its default (typically
                        ``AllPartitionMapping`` for unpartitioned
                        upstream → partitioned downstream).
    """

    upstream_step: str
    partition_rule: Optional[PartitionRule]
    upstream_library: Optional[str] = None
