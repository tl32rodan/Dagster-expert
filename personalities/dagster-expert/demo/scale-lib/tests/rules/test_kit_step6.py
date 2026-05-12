from pipelines.rules.kit_step6 import KitStep6Rule
from pipelines.spec.partition_rule import RootBranch
from pipelines.spec.step_taxonomy import kits


def test_non_kit_step_emits_nothing():
    r = KitStep6Rule()
    for s in ("step0", "step6", "phantom", "FunKits"):
        assert list(r.emit_edges("lib", s)) == []


def test_rln_emits_nothing_when_exempt():
    assert list(KitStep6Rule(rln_exempt=True).emit_edges("lib", "rln")) == []


def test_other_kits_emit_step6_root_branch():
    r = KitStep6Rule()
    for k in kits():
        if k == "rln":
            continue
        edges = list(r.emit_edges("lib", k))
        assert len(edges) == 1
        assert edges[0].upstream_step == "step6"
        assert isinstance(edges[0].partition_rule, RootBranch)
