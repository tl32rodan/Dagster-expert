from pipelines.rules.setup_gate import SetupGateRule
from pipelines.spec.partition_rule import RootBranch


def test_step0_emits_nothing():
    assert list(SetupGateRule().emit_edges("lib", "step0")) == []


def test_auto_download_emits_nothing():
    assert list(SetupGateRule().emit_edges("lib", "auto_download")) == []


def test_arbitrary_step_emits_root_branch_step0():
    edges = list(SetupGateRule().emit_edges("lib", "step3"))
    assert len(edges) == 1
    assert edges[0].upstream_step == "step0"
    assert isinstance(edges[0].partition_rule, RootBranch)


def test_kit_step_gets_setup_gate():
    edges = list(SetupGateRule().emit_edges("lib", "rln"))
    assert edges[0].upstream_step == "step0"
