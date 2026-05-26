"""Partition definitions as MODULE-LEVEL SINGLETONS (STANDARD_USAGE 9a:
one object per shape, imported everywhere; a fresh object per asset would
silently degrade cross-asset mapping to 'all partitions'). Keys come from
config/liberate.yaml, not hardcoded."""
from pathlib import Path

from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition

from core.config import load_config

CONFIG = load_config(Path(__file__).resolve().parents[2] / "config" / "liberate.yaml")
PVTS = CONFIG.pvt_keys
CELLS = CONFIG.cell_keys
SECTIONS = CONFIG.sections

pvt_partitions = StaticPartitionsDefinition(PVTS)
cell_partitions = StaticPartitionsDefinition(CELLS)
# 2 axes is the 1.13.3 hard limit; pvt x cell fits exactly.
pvt_x_cell = MultiPartitionsDefinition({"pvt": pvt_partitions, "cell": cell_partitions})
