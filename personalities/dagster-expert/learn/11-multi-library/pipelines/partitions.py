"""Partition definitions for the multi-library lesson.

Each library has the same shape as lesson 10's branched flow:
3 branches (corner / lvf / em), each with its own PVTRC subset.
"""

from dagster import StaticPartitionsDefinition

LIBRARIES = ["svt", "lvt"]                # Vt classes; expand to 6 in real use

CORNER_PVTRCS = ["ff_125", "tt_25", "ss_m40"]
LVF_PVTRCS    = ["tt_25"]
EM_PVTRCS     = ["ff_125", "ss_m40"]

assert set(LVF_PVTRCS).issubset(CORNER_PVTRCS)
assert set(EM_PVTRCS).issubset(CORNER_PVTRCS)

corner_partitions = StaticPartitionsDefinition(CORNER_PVTRCS)
lvf_partitions    = StaticPartitionsDefinition(LVF_PVTRCS)
em_partitions     = StaticPartitionsDefinition(EM_PVTRCS)

STEPS = [1, 2, 3]
BRANCHES = ["corner", "lvf", "em"]
