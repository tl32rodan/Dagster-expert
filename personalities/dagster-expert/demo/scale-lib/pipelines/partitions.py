"""Layer 4 — Dagster PartitionsDefinitions.

Three shapes covering all 21 steps:

* ``branch_partitions``       — 46 branches (extraction + char steps)
* ``root_branch_partitions``  — 1 partition: ``corner`` (setup + kits)
* (kit_singleton)             — no partition; used for aggregator kits if any

We deliberately do NOT add ``cell`` or ``PVT`` partition dimensions:
those live inside the step scripts (Tier 2). See ``CONTRACT.md``.
"""
from __future__ import annotations

from dagster import StaticPartitionsDefinition

from .spec.branch_hierarchy import default as default_hierarchy


_h = default_hierarchy()

branch_partitions = StaticPartitionsDefinition(list(_h.all_branches()))
"""Every branch — used by most extraction + char steps."""

root_branch_partitions = StaticPartitionsDefinition(list(_h.roots()))
"""Just the variant-tree roots (here: just ``corner``)."""
