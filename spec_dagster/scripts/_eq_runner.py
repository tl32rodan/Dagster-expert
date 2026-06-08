"""Subprocess worker for scripts/equivalence.py.

Runs ONE side (framework or hand-rolled) of the comparison in its own
process, against its own DAGSTER_HOME + LIBERATE_DAG_ROOT, and dumps a
JSON summary of behaviorally-relevant facts. The driver in
scripts/equivalence.py invokes this twice and diffs the JSONs.

Why a subprocess: each end imports its own `pipelines.*` module under
the same name; running them in the same process would collide.

Usage (called by equivalence.py, not by hand):
    python -m scripts._eq_runner --side framework  --out summary.json
    python -m scripts._eq_runner --side handrolled --out summary.json
"""
import argparse
import json
import os
import sys
from pathlib import Path

import dagster as dg


PVTS = ["tt_25", "ff_125", "ss_m40"]
CELLS = ["INV", "BUF", "NAND2"]


def _load_defs(side: str):
    if side == "framework":
        from flows.liberate_char.definitions import defs
        return defs
    # hand-rolled:cd into converted/ so its `pipelines` package is importable
    converted = Path("/home/user/Dagster-expert/personalities/flow-cartographer/"
                     "examples/liberate-char/converted")
    sys.path.insert(0, str(converted))
    os.chdir(converted)
    from pipelines.definitions import defs as _defs
    return _defs


def _by_name(defs):
    return {k.to_user_string(): a for a in defs.assets for k in a.keys}


def _mat(assets_def, instance, partition_key=None, name=None):
    res = {"pipes_subprocess_client": dg.PipesSubprocessClient()}
    r = dg.materialize(
        [assets_def], partition_key=partition_key, resources=res,
        selection=[name] if name else None, instance=instance,
    )
    assert r.success, f"materialize FAIL: {name} {partition_key}"
    return r


def _bootstrap_generators(by_name, instance, side: str):
    # framework: gen_sections writes 6 files (returns dict of 6 paths)
    # hand-rolled: section_tcl writes 6 files inside ONE asset body
    # Both end up the same on disk; here both call the asset once per pvt.
    for pvt in PVTS:
        _mat(by_name["template_tcl"], instance, partition_key=pvt, name="template_tcl")
        _mat(by_name["section_tcl"],  instance, partition_key=pvt, name="section_tcl")
        _mat(by_name["model_card"],   instance, partition_key=pvt, name="model_card")
    for cell in CELLS:
        _mat(by_name["netlist"], instance, partition_key=cell, name="netlist")
    _mat(by_name["cell_list"], instance, name="cell_list")
    _mat(by_name["main_tcl"],  instance, name="main_tcl")


def _materialize_all_characterize(by_name, instance):
    for pvt in PVTS:
        for cell in CELLS:
            # MultiPartitionKey serializes alphabetical: cell|pvt
            mk = dg.MultiPartitionKey({"pvt": pvt, "cell": cell})
            _mat(by_name["characterize"], instance, partition_key=mk, name="characterize")


def _collect_summary(defs, instance, side: str) -> dict:
    ag = defs.resolve_asset_graph()
    char_key = dg.AssetKey("characterize")
    node = ag.get(char_key)

    # 1) asset graph topology
    asset_names = sorted(k.to_user_string() for k in ag.get_all_asset_keys())
    parents = sorted(k.to_user_string() for k in node.parent_keys)

    # 2) partition shapes
    char_partitions = sorted(node.partitions_def.get_partition_keys())

    # 3) materialized state from event log
    materialized_keys = sorted(instance.get_materialized_partitions(char_key))

    # 4) data version per characterize partition (mock-liberate digest)
    # AND each upstream's input_data_version recorded on the same record —
    # this proves the dependency chain's versioning is identical too.
    # 1.13.3 key: dagster/data_version (NOT dagster/logical_version, NOT in
    # any standalone helper). See LESSONS.md L9 + L11.
    data_versions: dict[str, str] = {}
    input_versions: dict[str, dict[str, str]] = {}
    for pk in materialized_keys:
        recs = instance.get_event_records(
            event_records_filter=dg.EventRecordsFilter(
                event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=char_key,
                asset_partitions=[pk],
            ),
            limit=1, ascending=False,
        )
        if not recs:
            continue
        m = recs[0].event_log_entry.dagster_event.event_specific_data.materialization
        tags = (m.tags or {}) if m else {}
        data_versions[pk] = tags.get("dagster/data_version")
        ivs = {k.split("/", 2)[2]: v for k, v in tags.items()
               if k.startswith("dagster/input_data_version/")}
        input_versions[pk] = ivs

    # 5) characterize ASSET_MATERIALIZATION event count (1.13.3 requires
    # event_type on EventRecordsFilter — see LESSONS.md L10)
    mat_events = instance.get_event_records(
        event_records_filter=dg.EventRecordsFilter(
            event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=char_key,
        ),
        limit=10_000, ascending=True,
    )
    type_counts = {"ASSET_MATERIALIZATION": len(mat_events)}

    # 6) ldb digest per (pvt,cell) — the determinism contract
    out_dir = Path(os.environ["LIBERATE_DAG_ROOT"]) / "out"
    ldb_digests: dict[str, str] = {}
    for pvt in PVTS:
        for cell in CELLS:
            f = out_dir / f"{pvt}__{cell}.ldb"
            if not f.exists():
                continue
            for line in f.read_text().splitlines():
                if line.startswith("digest "):
                    ldb_digests[f"{cell}|{pvt}"] = line.split()[1]
                    break

    return {
        "side": side,
        "asset_names": asset_names,
        "characterize_parents": parents,
        "characterize_partitions": char_partitions,
        "materialized": materialized_keys,
        "data_versions": data_versions,
        "input_data_versions": input_versions,
        "event_type_counts": type_counts,
        "ldb_digests": ldb_digests,
    }


def _rerun_summary(by_name, instance, side: str) -> dict:
    """C2 single-partition rerun: re-materialize INV|tt_25 a second time;
    count NEW characterize ASSET_MATERIALIZATION events and see which
    partitions they belong to (must be exactly the one target)."""
    char_key = dg.AssetKey("characterize")
    f = dg.EventRecordsFilter(
        event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
        asset_key=char_key,
    )
    before = len(instance.get_event_records(event_records_filter=f, limit=10_000, ascending=True))
    mk = dg.MultiPartitionKey({"pvt": "tt_25", "cell": "INV"})
    _mat(by_name["characterize"], instance, partition_key=mk, name="characterize")
    after = instance.get_event_records(event_records_filter=f, limit=10_000, ascending=True)
    new_events = after[before:]
    touched = set()
    for r in new_events:
        ev = r.event_log_entry.dagster_event
        m = ev.event_specific_data.materialization if ev else None
        if m and m.partition:
            touched.add(m.partition)
    return {
        "rerun_new_materialization_count": len(new_events),
        "rerun_touched_partitions": sorted(touched),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--side", required=True, choices=["framework", "handrolled"])
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    defs = _load_defs(args.side)
    by_name = _by_name(defs)
    with dg.DagsterInstance.get() as instance:
        # storage migrate (C5): MUST be a no-op on fresh sqlite; failure is FAIL
        instance.upgrade()  # equivalent to `dagster instance migrate`
        _bootstrap_generators(by_name, instance, args.side)
        _materialize_all_characterize(by_name, instance)
        summary = _collect_summary(defs, instance, args.side)
        rerun = _rerun_summary(by_name, instance, args.side)
        summary["rerun"] = rerun

    Path(args.out).write_text(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
