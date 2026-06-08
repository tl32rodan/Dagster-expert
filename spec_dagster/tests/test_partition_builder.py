"""M2 partition shapes."""
from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition

from framework.spec.schema import DimensionSpec
from framework.assets.partition_builder import build_partitions_def

DIMS = {
    "pvt": DimensionSpec(type="static", values=["tt_25", "ff_125", "ss_m40"]),
    "cell": DimensionSpec(type="static", values=["INV", "BUF", "NAND2"]),
}


def test_unpartitioned():
    assert build_partitions_def([], DIMS) is None


def test_single_static():
    pd = build_partitions_def(["pvt"], DIMS)
    assert isinstance(pd, StaticPartitionsDefinition)
    assert set(pd.get_partition_keys()) == {"tt_25", "ff_125", "ss_m40"}


def test_multi_static():
    pd = build_partitions_def(["pvt", "cell"], DIMS)
    assert isinstance(pd, MultiPartitionsDefinition)
    assert len(pd.get_partition_keys()) == 9  # 3 pvt x 3 cell


def test_multi_partition_key_string_format():
    """MultiPartitionKey serializes dimension values joined by '|' in
    alphabetical dimension-name order (cell before pvt). The sensor's
    desired/observed set math depends on this format being consistent."""
    pd = build_partitions_def(["pvt", "cell"], DIMS)
    keys = set(pd.get_partition_keys())
    assert "INV|tt_25" in keys  # cell|pvt
