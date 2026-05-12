from pipelines.rules.step7_follow import Step7FollowRule
from pipelines.spec.partition_rule import SameBranch


def test_step7_emits_step1_same_branch():
    edges = list(Step7FollowRule().emit_edges("lib", "step7"))
    assert len(edges) == 1
    assert edges[0].upstream_step == "step1"
    assert isinstance(edges[0].partition_rule, SameBranch)


def test_other_steps_emit_nothing():
    for s in ("step1", "step2", "step6", "phantom", "rln"):
        assert list(Step7FollowRule().emit_edges("lib", s)) == []
