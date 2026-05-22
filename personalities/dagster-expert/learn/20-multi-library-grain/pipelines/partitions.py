"""Two ``PartitionsDefinition`` objects shared across all 100 libraries.

Re-using the SAME object across assets is critical: Dagster's default
``IdentityPartitionMapping`` requires the upstream and downstream to
share the same ``PartitionsDefinition`` *identity* (not just equal
content). If we constructed a fresh ``StaticPartitionsDefinition`` per
asset, identity mapping would silently fall back to all-partition
fan-out at reconciliation time.
"""
from __future__ import annotations

from dagster import StaticPartitionsDefinition

from .branches import all_branches, roots


# All 46 branches — used by EXTRACTION and CHAR steps.
branch_partitions = StaticPartitionsDefinition(list(all_branches()))


# Only the root branch (``corner``) — used by SETUP_ROOT_ONLY and
# KIT_ROOT_ONLY steps. These never branch out, so we save 45
# partition records per (library, step) by giving them their own def.
root_branch_partitions = StaticPartitionsDefinition(sorted(roots()))
