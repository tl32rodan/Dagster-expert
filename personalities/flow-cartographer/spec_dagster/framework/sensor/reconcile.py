"""M3 — observed-partition query (whitepaper §5.2).

The state source of truth is the event log, not a watermark. For a
multi-partitioned compute asset, the set of materialized partition keys
IS the `observed` set the reconciliation planner subtracts from desired.
"""
from __future__ import annotations

import dagster as dg


def observed_partitions(instance: dg.DagsterInstance, asset_name: str) -> set[str]:
    """Return the set of materialized partition keys for an asset.

    Uses instance.get_materialized_partitions (event-log backed; batched
    — one query, not per-partition). Empty set if never materialized.
    """
    try:
        return set(instance.get_materialized_partitions(dg.AssetKey(asset_name)))
    except Exception:
        # Asset never materialized / unknown -> nothing observed yet.
        return set()
