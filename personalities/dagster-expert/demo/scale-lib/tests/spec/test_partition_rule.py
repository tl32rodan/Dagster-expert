"""PartitionRule resolution — each rule isolated."""
from pipelines.spec.partition_rule import (
    FixedPartitions,
    ParentOfDownstream,
    RootBranch,
    SameBranch,
    UnionOf,
)


def test_same_branch_returns_only_self():
    r = SameBranch()
    assert r.resolve("em") == frozenset({"em"})
    assert r.resolve("tmsf_lde5") == frozenset({"tmsf_lde5"})


def test_fixed_partitions_ignores_downstream():
    r = FixedPartitions(keys=frozenset({"corner"}))
    assert r.resolve("em") == frozenset({"corner"})
    assert r.resolve("tmsf_lde1") == frozenset({"corner"})


def test_parent_of_downstream_immediate_with_self():
    r = ParentOfDownstream(include_self=True, to_root=False)
    assert r.resolve("tmsf_lde1_ht") == frozenset({"tmsf_lde1_ht", "tmsf_lde1"})
    assert r.resolve("mpwda_aged_lvf") == frozenset(
        {"mpwda_aged_lvf", "mpwda_aged"},
    )


def test_parent_of_downstream_no_self_returns_only_parent():
    r = ParentOfDownstream(include_self=False, to_root=False)
    assert r.resolve("tmsf_lde1_ht") == frozenset({"tmsf_lde1"})
    assert r.resolve("corner") == frozenset()      # root has no parent


def test_parent_of_downstream_to_root():
    r = ParentOfDownstream(include_self=False, to_root=True)
    assert r.resolve("tmsf_lde1_ht") == frozenset(
        {"tmsf_lde1", "tmsf_self", "corner"},
    )


def test_root_branch_always_returns_root():
    r = RootBranch()
    assert r.resolve("em") == frozenset({"corner"})
    assert r.resolve("tmsf_lde10_ht") == frozenset({"corner"})


def test_union_of_combines():
    r = UnionOf(rules=(
        SameBranch(),
        FixedPartitions(keys=frozenset({"corner"})),
    ))
    assert r.resolve("em") == frozenset({"em", "corner"})


def test_rules_are_hashable():
    # Used by the registry to dedupe.
    assert hash(SameBranch()) == hash(SameBranch())
    assert hash(FixedPartitions(frozenset({"a"}))) == hash(
        FixedPartitions(frozenset({"a"})),
    )
