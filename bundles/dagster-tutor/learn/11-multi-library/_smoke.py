"""End-to-end smoke driver for lesson 11 — multi-library branched flow."""

import os
import shutil
import sys
import time
from pathlib import Path

os.environ.setdefault("DAGSTER_HOME", "/tmp/dagster-11-test-home")
Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)
shutil.rmtree("/tmp/dagster-11-flow", ignore_errors=True)

from dagster import AssetKey, materialize  # noqa: E402

from pipelines.asset import defs  # noqa: E402
from pipelines.partitions import (  # noqa: E402
    BRANCHES,
    CORNER_PVTRCS,
    EM_PVTRCS,
    LIBRARIES,
    LVF_PVTRCS,
)


def _asset(*path):
    target = AssetKey(list(path))
    for a in defs.assets:
        if a.key == target:
            return a
    raise KeyError(path)


def _step(label: str, fn):
    print(f">>> {label}")
    t0 = time.time()
    fn()
    print(f"    ok  ({time.time() - t0:.1f}s)")


def materialize_partitions(asset_obj, pvtrcs):
    for p in pvtrcs:
        r = materialize([asset_obj], partition_key=p, resources=defs.resources)
        assert r.success, f"{asset_obj.key.path} @ {p} failed"


PVTRCS_PER_BRANCH = {
    "corner": CORNER_PVTRCS,
    "lvf":    LVF_PVTRCS,
    "em":     EM_PVTRCS,
}


def run_library(library: str):
    """Materialize one library's full subgraph in dep order."""
    for step in [1, 2, 3]:
        _step(
            f"  {library}/corner_step{step} ({len(CORNER_PVTRCS)} PVTRCs)",
            lambda l=library, s=step: materialize_partitions(
                _asset(l, f"corner_step{s}"), CORNER_PVTRCS),
        )
        _step(
            f"  {library}/lvf_step{step} ({len(LVF_PVTRCS)} PVTRCs)",
            lambda l=library, s=step: materialize_partitions(
                _asset(l, f"lvf_step{s}"), LVF_PVTRCS),
        )
        _step(
            f"  {library}/em_step{step} ({len(EM_PVTRCS)} PVTRCs)",
            lambda l=library, s=step: materialize_partitions(
                _asset(l, f"em_step{s}"), EM_PVTRCS),
        )
    _step(
        f"  {library}/lib_signoff",
        lambda l=library: materialize(
            [_asset(l, "lib_signoff")], resources=defs.resources),
    )


if __name__ == "__main__":
    overall = time.time()
    try:
        _step("step 0 — cell_list (shared)",
              lambda: materialize([_asset("cell_list")],
                                  resources=defs.resources))
        for lib in LIBRARIES:
            print(f"\n--- library: {lib} ---")
            run_library(lib)
        _step("\nstep N — cross_library_signoff",
              lambda: materialize([_asset("cross_library_signoff")],
                                  resources=defs.resources))
    except AssertionError as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== ALL STEPS PASS — total {time.time() - overall:.1f}s ===")

    out = Path("/tmp/dagster-11-flow")
    counts = {"cells.json": 1 if (out / "cells.json").exists() else 0}
    for lib in LIBRARIES:
        for branch, pvtrcs in PVTRCS_PER_BRANCH.items():
            for s in [1, 2, 3]:
                d = out / lib / branch / f"step{s}"
                counts[f"{lib}/{branch}/step{s}"] = len(list(d.glob("*.out")))
        counts[f"{lib}/lib_signoff"] = (
            1 if (out / lib / "signoff.tar").exists() else 0
        )
    counts["cross_library"] = (
        1 if (out / "cross_library_signoff.tar").exists() else 0
    )

    expected = {"cells.json": 1, "cross_library": 1}
    for lib in LIBRARIES:
        expected[f"{lib}/corner/step1"] = 3
        expected[f"{lib}/corner/step2"] = 3
        expected[f"{lib}/corner/step3"] = 3
        expected[f"{lib}/lvf/step1"] = 1
        expected[f"{lib}/lvf/step2"] = 1
        expected[f"{lib}/lvf/step3"] = 1
        expected[f"{lib}/em/step1"] = 2
        expected[f"{lib}/em/step2"] = 2
        expected[f"{lib}/em/step3"] = 2
        expected[f"{lib}/lib_signoff"] = 1

    print("\nartifact counts:")
    for k in expected:
        ok = "OK" if counts[k] == expected[k] else "MISMATCH"
        print(f"  {k}: got {counts[k]}, expected {expected[k]}  {ok}")
    assert counts == expected, f"mismatch: {counts} != {expected}"
    print("\nALL ARTIFACT COUNTS MATCH")
