"""Translator tests — verify PartitionRule -> Dagster PartitionMapping.

These tests import Dagster; they're slower than the spec tests but still
sub-second. They use real Dagster StaticPartitionsDefinition instances.
"""
from dagster import (
    AssetKey,
    IdentityPartitionMapping,
    SpecificPartitionsPartitionMapping,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
)

from pipelines.spec.branch_hierarchy import default as default_hierarchy
from pipelines.spec.dep_edge import DepEdge
from pipelines.spec.partition_rule import (
    FixedPartitions,
    ParentOfDownstream,
    RootBranch,
    SameBranch,
    UnionOf,
)
from pipelines.translator import (
    _PartitionRuleBackedMapping,
    to_asset_in,
    to_partition_mapping,
)


def _branch_pdef():
    return StaticPartitionsDefinition(list(default_hierarchy().all_branches()))


def test_same_branch_maps_to_identity():
    pm = to_partition_mapping(SameBranch())
    assert isinstance(pm, IdentityPartitionMapping)


def test_fixed_partitions_maps_to_specific():
    pm = to_partition_mapping(FixedPartitions(frozenset({"corner"})))
    assert isinstance(pm, SpecificPartitionsPartitionMapping)


def test_root_branch_maps_to_specific_corner():
    pm = to_partition_mapping(RootBranch())
    assert isinstance(pm, SpecificPartitionsPartitionMapping)


def test_parent_of_downstream_maps_to_static():
    pm = to_partition_mapping(ParentOfDownstream(include_self=False))
    assert isinstance(pm, StaticPartitionMapping)


def test_unionof_maps_to_static():
    rule = UnionOf(rules=(SameBranch(), ParentOfDownstream(include_self=False)))
    pm = to_partition_mapping(rule)
    assert isinstance(pm, StaticPartitionMapping)


def test_static_mapping_includes_tmsf_lde1_pair():
    """Under UnionOf(SameBranch, ParentOfDownstream), 'tmsf_lde1's parent
    is 'tmsf_self', so upstream 'tmsf_self' must include 'tmsf_lde1'
    in its downstream list."""
    rule = UnionOf(rules=(SameBranch(), ParentOfDownstream(include_self=False)))
    pm = to_partition_mapping(rule)
    d = pm.downstream_partition_keys_by_upstream_partition_key
    assert "tmsf_lde1" in d["tmsf_self"]


def test_static_mapping_corner_fans_out_to_immediate_children():
    """upstream 'corner' should map to {corner, em, ht, lvf, mpwda, tmsf_self}."""
    rule = UnionOf(rules=(SameBranch(), ParentOfDownstream(include_self=False)))
    pm = to_partition_mapping(rule)
    d = pm.downstream_partition_keys_by_upstream_partition_key
    corner_targets = set(d["corner"])
    assert corner_targets == {"corner", "em", "ht", "lvf", "mpwda", "tmsf_self"}


def test_static_mapping_root_is_self_only_for_root():
    """corner downstream's upstream resolves to {corner} only."""
    rule = UnionOf(rules=(SameBranch(), ParentOfDownstream(include_self=False)))
    pm = to_partition_mapping(rule)
    d = pm.downstream_partition_keys_by_upstream_partition_key
    # corner is never in any non-corner upstream's downstream list except its own.
    # Confirm corner appears as a downstream only of upstream 'corner'.
    upstreams_for_corner = [
        up for up, downs in d.items() if "corner" in downs
    ]
    assert upstreams_for_corner == ["corner"]


def test_to_asset_in_default_input_name_same_library():
    edge = DepEdge(upstream_step="step4", partition_rule=SameBranch())
    name, asset_in = to_asset_in("lib_a", edge)
    assert name == "in_step4"
    assert asset_in.key == AssetKey(["lib_a", "step4"])


def test_to_asset_in_cross_library():
    edge = DepEdge(
        upstream_step="step6",
        upstream_library="lib_a",
        partition_rule=RootBranch(),
    )
    name, asset_in = to_asset_in("lib_b", edge)
    assert name == "in_lib_a_step6"
    assert asset_in.key == AssetKey(["lib_a", "step6"])
