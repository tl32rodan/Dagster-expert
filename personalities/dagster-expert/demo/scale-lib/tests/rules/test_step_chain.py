from pipelines.rules.step_chain import StepChainRule
from pipelines.spec.partition_rule import SameBranch


def test_step3_emits_step2_via_same_branch():
    r = StepChainRule()
    edges = list(r.emit_edges("lib_a", "step3"))
    assert len(edges) == 1
    assert edges[0].upstream_step == "step2"
    assert isinstance(edges[0].partition_rule, SameBranch)
    assert edges[0].upstream_library is None


def test_step2_emits_nothing_first_in_chain():
    assert list(StepChainRule().emit_edges("lib_a", "step2")) == []


def test_non_chain_step_emits_nothing():
    for s in ("step0", "phantom", "step7", "FunKits", "rln"):
        assert list(StepChainRule().emit_edges("lib_a", s)) == []


def test_step6_emits_step5():
    edges = list(StepChainRule().emit_edges("lib_a", "step6"))
    assert [(e.upstream_step, type(e.partition_rule).__name__) for e in edges] == [
        ("step5", "SameBranch"),
    ]
