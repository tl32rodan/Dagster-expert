"""Pin the step inventory and chain ordering."""
from pipelines.spec import step_taxonomy as st


def test_chain_is_step2_through_step6():
    assert st.chain() == ("step2", "step3", "step4", "step5", "step6")


def test_prev_in_chain():
    assert st.prev_in_chain("step2") is None
    assert st.prev_in_chain("step3") == "step2"
    assert st.prev_in_chain("step6") == "step5"
    assert st.prev_in_chain("phantom") is None  # not in chain


def test_kits_root_only_count_is_9():
    assert len(st.kits()) == 9


def test_root_only_classification():
    assert st.is_root_only("step0")
    assert st.is_root_only("auto_download")
    assert st.is_root_only("rln")
    assert st.is_root_only("meta")
    assert not st.is_root_only("step1")
    assert not st.is_root_only("step5")
    assert not st.is_root_only("phantom")


def test_step_count():
    # 2 setup + 4 extraction + 6 char + 9 kits = 21 logical steps.
    assert len(st.step_names()) == 21
