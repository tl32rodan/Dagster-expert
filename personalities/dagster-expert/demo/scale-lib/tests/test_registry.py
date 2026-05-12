"""Pin DEPS resolution for the demo's intended workload."""
from pipelines.registry import DEPS
from pipelines.spec.partition_rule import (
    ParentOfDownstream,
    RootBranch,
    SameBranch,
    UnionOf,
)


def _edges_dict(library, step):
    """Returns {(up_lib, up_step): partition_rule} for easy assertion."""
    return {
        (e.upstream_library, e.upstream_step): e.partition_rule
        for e in DEPS.edges_for(library, step)
    }


def test_step0_only_has_no_deps():
    assert _edges_dict("lib", "step0") == {}


def test_auto_download_no_deps():
    assert _edges_dict("lib", "auto_download") == {}


def test_step1_has_setup_gate_only():
    edges = _edges_dict("lib", "step1")
    assert set(edges.keys()) == {(None, "step0")}
    assert isinstance(edges[(None, "step0")], RootBranch)


def test_step3_has_setup_gate_plus_chain():
    edges = _edges_dict("lib", "step3")
    assert set(edges.keys()) == {(None, "step0"), (None, "step2")}
    assert isinstance(edges[(None, "step0")], RootBranch)
    assert isinstance(edges[(None, "step2")], SameBranch)


def test_step5_merges_chain_and_parent_mirror_into_unionof():
    """step5 gets:
      - SetupGate: step0 via RootBranch
      - StepChain: step4 via SameBranch
      - ParentMirror: step4 via ParentOfDownstream(include_self=False)
    The two step4 edges are merged into UnionOf.
    """
    edges = _edges_dict("lib", "step5")
    assert set(edges.keys()) == {(None, "step0"), (None, "step4")}
    step4_rule = edges[(None, "step4")]
    assert isinstance(step4_rule, UnionOf)
    rule_types = {type(r).__name__ for r in step4_rule.rules}
    assert rule_types == {"SameBranch", "ParentOfDownstream"}


def test_step5_resolution_for_non_root_branch():
    """The unioned rule on step4 for downstream tmsf_lde1 should pull
    BOTH tmsf_lde1's own step4 AND tmsf_self's step4 (its parent).
    """
    step4_rule = _edges_dict("lib", "step5")[(None, "step4")]
    assert step4_rule.resolve("tmsf_lde1") == frozenset(
        {"tmsf_lde1", "tmsf_self"},
    )


def test_step5_resolution_for_root_branch_is_self_only():
    step4_rule = _edges_dict("lib", "step5")[(None, "step4")]
    # corner has no parent -> ParentOfDownstream contributes nothing -> just self.
    assert step4_rule.resolve("corner") == frozenset({"corner"})


def test_step7_has_chain_step1_and_setup_gate():
    edges = _edges_dict("lib", "step7")
    assert set(edges.keys()) == {(None, "step0"), (None, "step1")}
    assert isinstance(edges[(None, "step1")], SameBranch)


def test_kit_rln_has_only_step0_root():
    edges = _edges_dict("lib", "rln")
    # SetupGate + KitRln both target step0/RootBranch -> UnionOf(RootBranch, RootBranch)
    # OR merged to single RootBranch. Either way: only one upstream key.
    assert set(edges.keys()) == {(None, "step0")}


def test_kit_meta_depends_on_step6_and_step0():
    edges = _edges_dict("lib", "meta")
    assert set(edges.keys()) == {(None, "step0"), (None, "step6")}
    assert isinstance(edges[(None, "step6")], RootBranch)


def test_phantom_only_has_setup_gate():
    edges = _edges_dict("lib", "phantom")
    assert set(edges.keys()) == {(None, "step0")}


def test_FunKits_only_has_setup_gate():
    edges = _edges_dict("lib", "FunKits")
    assert set(edges.keys()) == {(None, "step0")}


def test_with_rules_does_not_mutate_DEPS():
    from pipelines.rules.cross_library import CrossLibraryRule
    extra = CrossLibraryRule(
        target_library="lib_b",
        target_step="step1",
        source_library="lib_a",
        source_step="step6",
        partition_rule=RootBranch(),
    )
    extended = DEPS.with_rules([extra])
    # Original DEPS unaffected
    assert _edges_dict("lib_b", "step1") == {(None, "step0"): _edges_dict("lib_b", "step1")[(None, "step0")]}
    # Extended sees the new edge
    edges = {
        (e.upstream_library, e.upstream_step): e.partition_rule
        for e in extended.edges_for("lib_b", "step1")
    }
    assert ("lib_a", "step6") in edges
