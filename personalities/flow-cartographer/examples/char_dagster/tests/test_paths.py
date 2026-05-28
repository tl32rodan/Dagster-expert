"""Path helpers — derive_lpe_rc + constructed paths."""
import pytest

from char_dagster.config import load_config
from char_dagster.paths import (
    SECTION_NUMBERS,
    all_section_files_for_partition,
    derive_lpe_rc,
    model_card_path,
    netlist_path,
    section_tcl_path,
    template_tcl_path,
)


def test_derive_lpe_rc():
    assert derive_lpe_rc("LPE_ssgnp_cworst_T_25c") == "LPE_cworst_T_25c"
    assert derive_lpe_rc("LPE_typgnp_cworst_T_25c") == "LPE_cworst_T_25c"


def test_derive_lpe_rc_rejects_bad_input():
    with pytest.raises(ValueError):
        derive_lpe_rc("not_an_lpe_string")
    with pytest.raises(ValueError):
        derive_lpe_rc("LPE")


def test_all_section_files_for_partition_count(tmp_path):
    cfg = load_config(
        # use the shipped demo config
        __file__.replace("tests/test_paths.py", "config/char_config.yaml")
    )
    files = all_section_files_for_partition(cfg, "LPE_ssgnp_cworst_T_25c", "tt_25")
    assert len(files) == 1 + len(SECTION_NUMBERS) == 7
    # first file is the Template tcl, rest are SECTION_{2..7}/char_<pvt>.tcl
    assert "Template" in str(files[0])
    for f, n in zip(files[1:], SECTION_NUMBERS):
        assert f"SECTION_{n}" in str(f)
        assert "char_tt_25.tcl" in str(f)


def test_paths_use_trio_group_subtree():
    cfg = load_config(
        __file__.replace("tests/test_paths.py", "config/char_config.yaml")
    )
    tg = "LPE_ssgnp_cworst_T_25c"
    assert tg in str(model_card_path(cfg, tg, "tt_25"))
    assert tg in str(netlist_path(cfg, tg, "INV"))
    assert tg in str(template_tcl_path(cfg, tg, "tt_25"))
    assert tg in str(section_tcl_path(cfg, tg, "tt_25", 3))
