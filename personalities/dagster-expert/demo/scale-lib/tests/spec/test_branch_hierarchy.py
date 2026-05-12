"""Pin the variant-tree shape. These tests fail loudly when
config/branches.yaml drifts.
"""
from pipelines.spec import branch_hierarchy as bh


def test_root_is_corner_and_unique():
    h = bh.default()
    assert h.roots() == ("corner",)
    assert h.is_root("corner")
    assert h.parent_of("corner") is None


def test_standard_family_parents_to_corner():
    h = bh.default()
    for b in ("em", "ht", "lvf"):
        assert h.parent_of(b) == "corner"
    # lvf_ht is _ht of lvf -> parent is lvf (per project rule "_ht
    # mirrors non-_ht of the same prefix")
    assert h.parent_of("lvf_ht") == "lvf"


def test_aged_family_parent_chain():
    h = bh.default()
    assert h.parent_of("mpwda") == "corner"
    assert h.parent_of("mpwda_aged") == "mpwda"
    assert h.parent_of("mpwda_lvf") == "mpwda"
    assert h.parent_of("mpwda_aged_lvf") == "mpwda_aged"


def test_tmsf_self_family():
    h = bh.default()
    assert h.parent_of("tmsf_self") == "corner"
    assert h.parent_of("tmsf_self_ht") == "tmsf_self"
    assert h.parent_of("tmsf_self_lvf") == "tmsf_self"
    assert h.parent_of("tmsf_self_lvf_ht") == "tmsf_self_lvf"


def test_tmsf_lde_branches_all_mirror_tmsf_self():
    h = bh.default()
    for i in range(1, 24):
        assert h.parent_of(f"tmsf_lde{i}") == "tmsf_self"


def test_tmsf_lde_ht_mirrors_non_ht_counterpart():
    h = bh.default()
    for i in range(1, 11):
        assert h.parent_of(f"tmsf_lde{i}_ht") == f"tmsf_lde{i}"


def test_ancestors_walk_to_root():
    h = bh.default()
    assert h.ancestors_of("tmsf_lde1_ht") == ("tmsf_lde1", "tmsf_self", "corner")
    assert h.ancestors_of("mpwda_aged_lvf") == ("mpwda_aged", "mpwda", "corner")
    assert h.ancestors_of("corner") == ()


def test_descendants_of_corner_is_everyone_else():
    h = bh.default()
    descendants = set(h.descendants_of("corner"))
    others = set(h.all_branches()) - {"corner"}
    assert descendants == others


def test_total_count_is_46():
    h = bh.default()
    # Sanity check on the demo scale (per plan §2.1).
    assert len(h.all_branches()) == 46


def test_no_cycles():
    h = bh.default()
    for b in h.all_branches():
        # ancestors_of raises on cycle; just calling it is enough.
        h.ancestors_of(b)
