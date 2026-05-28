"""Partition mappings as DATA → translated to BUILT-IN mappings, memoized
as module-level singletons.

Hard rules (STANDARD_USAGE 3.2 + 1.13.3 Gotcha #4):
  * NEVER subclass ``PartitionMapping`` — breaks reconciliation and
    auto-materialize.
  * 2D-downstream  <- 1D-upstream      → ``MultiToSingleDimensionPartitionMapping``
  * 2D-downstream  <- 2D-upstream
        same-dims  (identity)          → omit ``partition_mapping=`` (default)
        diff-dim   (one shared, one    → ``StaticPartitionMapping`` with a
                    differing)            pre-computed table. Built-in
                                          ``MultiPartitionMapping`` is listed
                                          as reconciliation-safe but the 1.13.3
                                          corpus has no concrete signature
                                          example, so we use the static form
                                          which is verified safe.
  * Unpartitioned <- partitioned       → ``AllPartitionMapping``
  * Partitioned   <- unpartitioned     → default (Dagster fans single
                                          upstream to all downstream).
"""
from collections import defaultdict

from dagster import (
    AllPartitionMapping,
    MultiToSingleDimensionPartitionMapping,
    StaticPartitionMapping,
)

from char_dagster.partitions import CELLS, PVTS, TRIO_GROUPS

# ---------------------------------------------------------------------------
# netlist_files (trio_group × cell)  →  pvt_section_files (trio_group × pvt)
# Each (trio_group, pvt) section file depends on ALL cells of the SAME
# trio_group. Same trio_group, differing dim (cell → pvt).
# ---------------------------------------------------------------------------

def _build_netlist_to_section_mapping() -> StaticPartitionMapping:
    """Pre-compute {netlist_key -> {section_keys}} once at definition time."""
    out: dict = defaultdict(set)
    for tg in TRIO_GROUPS:
        for cell in CELLS:
            up_key = f"{tg}|{cell}"      # MultiPartitionKey serialises as "dim1|dim2"
            for pvt in PVTS:
                out[up_key].add(f"{tg}|{pvt}")
    return StaticPartitionMapping(
        downstream_partition_keys_by_upstream_partition_key={
            k: sorted(v) for k, v in out.items()
        }
    )


NETLIST_TO_SECTION = _build_netlist_to_section_mapping()

# ---------------------------------------------------------------------------
# mnpvt_cell_list_tcl (trio_group) → main_char_script (unpartitioned)
# pvt_section_files (trio_group × pvt) → main_char_script (unpartitioned)
# ---------------------------------------------------------------------------

ALL_FROM_ANY = AllPartitionMapping()

# ---------------------------------------------------------------------------
# Reserved: 1D-into-2D pull (matches liberate-char/converted pattern). Not
# used at the current asset shape but kept here for the internal agent to
# wire up if e.g. a per-pvt source asset is added later.
# ---------------------------------------------------------------------------

PVT_FROM_2D = MultiToSingleDimensionPartitionMapping(partition_dimension_name="pvt")
TRIO_GROUP_FROM_2D = MultiToSingleDimensionPartitionMapping(
    partition_dimension_name="trio_group"
)
