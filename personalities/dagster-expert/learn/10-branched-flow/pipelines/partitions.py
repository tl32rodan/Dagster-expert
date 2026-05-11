"""Partition definitions per branch.

In TSMC AP characterization, "branch" = channel type (corner / lvf
/ em), each with its OWN PVTRC sub-list. Branches run in parallel
columns; same-step lvf/em depend on same-step corner at the
matching PVTRC.

PVTRC = Process-Voltage-Temperature-RC corner. The single string
encodes all four (e.g. "ff_125" = fast process, 1.1V, 125C, fast RC).
"""

from dagster import StaticPartitionsDefinition

# Full PVTRC set for the corner branch (canonical reference)
CORNER_PVTRCS = ["ff_125", "tt_25", "ss_m40"]

# lvf typically runs at typical only — variation analysis doesn't
# need full corner coverage
LVF_PVTRCS = ["tt_25"]

# em runs at the two extremes for stress analysis
EM_PVTRCS = ["ff_125", "ss_m40"]

# Sanity: lvf and em PVTRCs must be subsets of corner PVTRCs (so
# the cross-branch lookup against corner outputs is well-defined)
assert set(LVF_PVTRCS).issubset(CORNER_PVTRCS), \
    "LVF_PVTRCS must be a subset of CORNER_PVTRCS"
assert set(EM_PVTRCS).issubset(CORNER_PVTRCS), \
    "EM_PVTRCS must be a subset of CORNER_PVTRCS"

corner_partitions = StaticPartitionsDefinition(CORNER_PVTRCS)
lvf_partitions    = StaticPartitionsDefinition(LVF_PVTRCS)
em_partitions     = StaticPartitionsDefinition(EM_PVTRCS)

STEPS = [1, 2, 3]
BRANCHES = ["corner", "lvf", "em"]
