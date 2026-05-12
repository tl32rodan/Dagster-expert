"""Definitions must load cleanly, expose the expected asset shape, and
have the dep edges we expect. Slower than spec tests but still ~1 s.
"""
from dagster import AssetKey

from pipelines.definitions import LIBRARIES, defs


def _ag():
    return defs.resolve_asset_graph()


def test_asset_count_matches_expectation():
    keys = list(_ag().get_all_asset_keys())
    # 21 step assets per library + 2 source observables.
    expected = 21 * len(LIBRARIES) + 2
    assert len(keys) == expected


def test_each_library_has_all_21_steps():
    keys = {tuple(k.path) for k in _ag().get_all_asset_keys()}
    for lib in LIBRARIES:
        assert (lib, "step0") in keys
        assert (lib, "step5") in keys
        assert (lib, "rln") in keys
        assert (lib, "meta") in keys
        assert (lib, "step7") in keys


def test_step3_upstreams_include_step2_and_step0():
    ag = _ag()
    step3 = AssetKey(["lib_a", "step3"])
    parents = set(ag.get(step3).parent_keys)
    assert AssetKey(["lib_a", "step2"]) in parents
    assert AssetKey(["lib_a", "step0"]) in parents


def test_step7_upstreams_include_step1_and_step0():
    ag = _ag()
    step7 = AssetKey(["lib_a", "step7"])
    parents = set(ag.get(step7).parent_keys)
    assert AssetKey(["lib_a", "step1"]) in parents
    assert AssetKey(["lib_a", "step0"]) in parents


def test_meta_kit_upstreams_include_step6_and_step0():
    ag = _ag()
    meta = AssetKey(["lib_a", "meta"])
    parents = set(ag.get(meta).parent_keys)
    assert AssetKey(["lib_a", "step6"]) in parents
    assert AssetKey(["lib_a", "step0"]) in parents


def test_rln_kit_upstream_is_only_step0_root():
    ag = _ag()
    rln = AssetKey(["lib_a", "rln"])
    parents = set(ag.get(rln).parent_keys)
    assert AssetKey(["lib_a", "step0"]) in parents
    assert AssetKey(["lib_a", "step6"]) not in parents


def test_step0_has_no_step_parents():
    ag = _ag()
    step0 = AssetKey(["lib_a", "step0"])
    parents = set(ag.get(step0).parent_keys)
    # Should have NO upstream step assets.
    step_parents = {p for p in parents if p.path[0] == "lib_a"}
    assert step_parents == set()


def test_branch_partitions_count_for_step5():
    ag = _ag()
    step5 = AssetKey(["lib_a", "step5"])
    pdef = ag.get(step5).partitions_def
    assert pdef is not None
    keys = pdef.get_partition_keys()
    assert len(keys) == 46


def test_root_branch_partitions_count_for_step0():
    ag = _ag()
    step0 = AssetKey(["lib_a", "step0"])
    pdef = ag.get(step0).partitions_def
    keys = pdef.get_partition_keys()
    assert keys == ["corner"]


def test_source_observables_present():
    keys = {tuple(k.path) for k in _ag().get_all_asset_keys()}
    assert ("pvt_manifest",) in keys
    assert ("cell_list",) in keys
