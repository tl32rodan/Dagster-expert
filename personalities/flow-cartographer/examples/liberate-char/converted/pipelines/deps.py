"""`characterize` (2D pvt x cell) depends on upstreams partitioned by ONE
dimension (or unpartitioned). Relationships are expressed as DATA, then
translated to BUILT-IN partition mappings, memoized as singletons.

This is the crux a weak agent gets wrong. The rules:
  * 2D-downstream <- 1D-upstream  => MultiToSingleDimensionPartitionMapping(dim)
    (a permitted built-in per STANDARD_USAGE 3.2; beta-flagged but supported)
  * 2D-downstream <- unpartitioned => no mapping (default)
  * NEVER subclass PartitionMapping (breaks reconciliation / auto-materialize).
"""
from dagster import AssetDep, AssetKey, MultiToSingleDimensionPartitionMapping

# DATA: (upstream asset name, selecting dimension | None if unpartitioned)
CHARACTERIZE_UPSTREAMS = [
    ("template_tcl", "pvt"),
    ("section_tcl", "pvt"),
    ("model_card", "pvt"),
    ("netlist", "cell"),
    ("cell_list", None),
    ("main_tcl", None),
]

# TRANSLATION: one built-in mapping object per dimension, built ONCE (memoized).
_DIMENSION_MAPPING = {
    "pvt": MultiToSingleDimensionPartitionMapping(partition_dimension_name="pvt"),
    "cell": MultiToSingleDimensionPartitionMapping(partition_dimension_name="cell"),
}


def characterize_deps():
    deps = []
    for name, dim in CHARACTERIZE_UPSTREAMS:
        mapping = _DIMENSION_MAPPING[dim] if dim is not None else None
        deps.append(AssetDep(AssetKey(name), partition_mapping=mapping))
    return deps
