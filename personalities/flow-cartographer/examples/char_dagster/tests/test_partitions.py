"""Partition definitions + mapping sanity. Requires Dagster (1.13.3)."""
import pytest

pytest.importorskip("dagster")

from char_dagster.partitions import (
    CELLS,
    PVTS,
    TRIO_GROUPS,
    cell_partitions,
    pvt_partitions,
    trio_group_partitions,
    trio_x_cell,
    trio_x_pvt,
)
from char_dagster.spec.mappings import NETLIST_TO_SECTION


def test_partition_singletons_have_expected_keys():
    assert set(trio_group_partitions.get_partition_keys()) == set(TRIO_GROUPS)
    assert set(pvt_partitions.get_partition_keys()) == set(PVTS)
    assert set(cell_partitions.get_partition_keys()) == set(CELLS)


def test_trio_x_pvt_cardinality():
    keys = trio_x_pvt.get_partition_keys()
    assert len(keys) == len(TRIO_GROUPS) * len(PVTS)


def test_trio_x_cell_cardinality():
    keys = trio_x_cell.get_partition_keys()
    assert len(keys) == len(TRIO_GROUPS) * len(CELLS)


def test_netlist_to_section_mapping_shape():
    """Each upstream (trio_group, cell) key should map to all (trio_group, pvt)
    section keys with the SAME trio_group — never crosses trio_groups."""
    table = NETLIST_TO_SECTION.downstream_partition_keys_by_upstream_partition_key
    for up_key, down_keys in table.items():
        up_tg = up_key.split("|")[0]
        assert len(down_keys) == len(PVTS), (
            f"{up_key} -> {len(down_keys)} sections, expected {len(PVTS)}"
        )
        for d in down_keys:
            assert d.split("|")[0] == up_tg, (
                f"mapping crosses trio_groups: {up_key} -> {d}"
            )
