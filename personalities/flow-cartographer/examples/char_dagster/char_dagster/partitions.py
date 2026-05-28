"""Partition definitions as MODULE-LEVEL SINGLETONS (STANDARD_USAGE 9a:
one object per shape, imported everywhere; a fresh object per asset
silently degrades cross-asset mapping to 'all partitions'). Keys come from
``config/char_config.yaml``, never hardcoded.

Shapes used in this project (2-dim is the 1.13.3 hard limit):
    trio_group       (1D)  -> mnpvt_cell_list_tcl
    trio_group x pvt (2D)  -> model_card_files, pvt_section_files,
                              characterization_run, validation_check
    trio_group x cell(2D)  -> netlist_files
"""
from pathlib import Path

from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition

from char_dagster.config import load_config

CONFIG = load_config(Path(__file__).resolve().parents[1] / "config" / "char_config.yaml")
TRIO_GROUPS = CONFIG.trio_groups
PVTS = CONFIG.pvt_names              # active-only
CELLS = CONFIG.cells

trio_group_partitions = StaticPartitionsDefinition(TRIO_GROUPS)
pvt_partitions = StaticPartitionsDefinition(PVTS)
cell_partitions = StaticPartitionsDefinition(CELLS)

trio_x_pvt = MultiPartitionsDefinition({
    "trio_group": trio_group_partitions,
    "pvt": pvt_partitions,
})
trio_x_cell = MultiPartitionsDefinition({
    "trio_group": trio_group_partitions,
    "cell": cell_partitions,
})
