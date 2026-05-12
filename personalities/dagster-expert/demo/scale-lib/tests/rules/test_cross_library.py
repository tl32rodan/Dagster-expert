from pipelines.rules.cross_library import CrossLibraryRule
from pipelines.spec.partition_rule import RootBranch


def test_emits_only_for_target_library_and_step():
    r = CrossLibraryRule(
        target_library="lib_b",
        target_step="step1",
        source_library="lib_a",
        source_step="step6",
        partition_rule=RootBranch(),
    )
    edges = list(r.emit_edges("lib_b", "step1"))
    assert len(edges) == 1
    assert edges[0].upstream_step == "step6"
    assert edges[0].upstream_library == "lib_a"


def test_silent_for_other_libraries():
    r = CrossLibraryRule(
        target_library="lib_b",
        target_step="step1",
        source_library="lib_a",
        source_step="step6",
        partition_rule=RootBranch(),
    )
    assert list(r.emit_edges("lib_a", "step1")) == []
    assert list(r.emit_edges("lib_b", "step5")) == []
