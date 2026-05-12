from pipelines.rules.parent_mirror import ParentMirrorRule
from pipelines.spec.partition_rule import ParentOfDownstream


def test_no_applies_to_emits_nothing():
    r = ParentMirrorRule()
    for step in ("step5", "step6", "step2"):
        assert list(r.emit_edges("lib", step)) == []


def test_step5_in_applies_to_emits_parent_of_step4():
    r = ParentMirrorRule(applies_to=frozenset({"step5"}))
    edges = list(r.emit_edges("lib", "step5"))
    assert len(edges) == 1
    assert edges[0].upstream_step == "step4"
    pr = edges[0].partition_rule
    assert isinstance(pr, ParentOfDownstream)
    assert pr.include_self is False
    assert pr.to_root is False


def test_non_chain_step_in_applies_to_emits_nothing():
    # FunKits is not in the step2..6 chain; can't mirror on something with no prev_in_chain
    r = ParentMirrorRule(applies_to=frozenset({"FunKits"}))
    assert list(r.emit_edges("lib", "FunKits")) == []


def test_resolution_for_non_root_branch():
    r = ParentMirrorRule(applies_to=frozenset({"step5"}))
    edge = next(iter(r.emit_edges("lib", "step5")))
    assert edge.partition_rule.resolve("tmsf_lde1") == frozenset({"tmsf_self"})
    assert edge.partition_rule.resolve("em") == frozenset({"corner"})


def test_resolution_for_root_branch_is_empty():
    # corner has no parent, so resolve returns {} — no extra dep beyond
    # the SameBranch edge that StepChainRule already emits.
    r = ParentMirrorRule(applies_to=frozenset({"step5"}))
    edge = next(iter(r.emit_edges("lib", "step5")))
    assert edge.partition_rule.resolve("corner") == frozenset()
