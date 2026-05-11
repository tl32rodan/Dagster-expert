"""Partition definitions for the real-flow lesson.

1.13.3 limits MultiPartitions to 2 dimensions. Step 1 (LPE) wants
3 dims (corner × volt_temp × cell), so we collapse vt + cell into
a composite "vt_cell" axis.
"""

from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition

CORNERS = ["ff", "tt", "ss"]

VOLT_TEMPS = [
    "0p72v__m40c",
    "0p9v__25c",
    "0p9v__m40c",
    "1p1v__25c",
    "1p1v__125c",
    "1p32v__125c",
]

CELLS = ["INV", "BUF", "NAND2", "MUX2"]

VT_CELLS = [f"{vt}__{c}" for vt in VOLT_TEMPS for c in CELLS]  # 24

corner_partitions = StaticPartitionsDefinition(CORNERS)

# Step 4 char step: 18 partitions (3 × 6)
pvt_partitions = MultiPartitionsDefinition({
    "corner":    StaticPartitionsDefinition(CORNERS),
    "volt_temp": StaticPartitionsDefinition(VOLT_TEMPS),
})

# Step 1 LPE: 72 partitions (3 × 24, vt+cell collapsed)
lpe_partitions = MultiPartitionsDefinition({
    "corner":  StaticPartitionsDefinition(CORNERS),
    "vt_cell": StaticPartitionsDefinition(VT_CELLS),
})


def split_vt_cell(vt_cell: str) -> tuple[str, str]:
    """`0p9v__25c__INV` → `("0p9v__25c", "INV")`."""
    parts = vt_cell.split("__")
    # vt is "Xv__Yc" (2 underscores), cell is the last part
    return "__".join(parts[:-1]), parts[-1]
