"""M2 mapping direction correctness (whitepaper appendix A item 1)."""
from dagster import (
    AllPartitionMapping,
    MultiToSingleDimensionPartitionMapping,
)

from framework.assets.mapping_builder import build_mapping


def test_unpartitioned_upstream_no_mapping():
    # cell_list (unpartitioned) -> characterize (pvt, cell)
    assert build_mapping("all", [], ["pvt", "cell"]) is None
    assert build_mapping("identity", [], ["pvt", "cell"]) is None


def test_single_dim_into_multi_dim_identity_dict():
    # template_tcl (pvt) -> characterize (pvt, cell)
    m = build_mapping({"pvt": "identity"}, ["pvt"], ["pvt", "cell"])
    assert isinstance(m, MultiToSingleDimensionPartitionMapping)
    assert m.partition_dimension_name == "pvt"


def test_single_dim_into_multi_dim_identity_string():
    # netlist (cell) -> characterize (pvt, cell), declared as bare "identity"
    m = build_mapping("identity", ["cell"], ["pvt", "cell"])
    assert isinstance(m, MultiToSingleDimensionPartitionMapping)
    assert m.partition_dimension_name == "cell"


def test_same_shape_multidim_is_default_identity():
    assert build_mapping("identity", ["pvt", "cell"], ["pvt", "cell"]) is None


def test_same_shape_singledim_is_default_identity():
    # same-shape single-dim => None (Dagster's default for same-partitioned
    # assets IS identity; no explicit mapping object needed)
    assert build_mapping("identity", ["pvt"], ["pvt"]) is None


def test_all_on_partitioned_upstream():
    assert isinstance(build_mapping("all", ["pvt"], ["pvt"]), AllPartitionMapping)


def test_unsupported_dict_rejected():
    import pytest
    with pytest.raises(ValueError):
        build_mapping({"pvt": "weird_rule"}, ["pvt"], ["pvt", "cell"])
