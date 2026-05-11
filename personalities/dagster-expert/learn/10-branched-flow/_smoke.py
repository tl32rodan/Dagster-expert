"""End-to-end smoke driver for lesson 10 — branched flow.

Critical ordering: within each step, corner's PVTRCs MUST be
materialized BEFORE lvf/em that read corner's output. Within each
branch, step N must run before step N+1.
"""

import os
import shutil
import sys
import time
from pathlib import Path

os.environ.setdefault("DAGSTER_HOME", "/tmp/dagster-10-test-home")
Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)
shutil.rmtree("/tmp/dagster-10-flow", ignore_errors=True)

from dagster import AssetKey, materialize  # noqa: E402

from pipelines.asset import defs  # noqa: E402
from pipelines.partitions import (  # noqa: E402
    BRANCHES,
    CORNER_PVTRCS,
    EM_PVTRCS,
    LVF_PVTRCS,
)


def _asset(name: str):
    target = AssetKey(name)
    for a in defs.assets:
        if a.key == target:
            return a
    raise KeyError(name)


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


if __name__ == "__main__":
    overall = time.time()
    try:
        _step("step 0 — cell_list",
              lambda: materialize([_asset("cell_list")],
                                  resources=defs.resources))

        # IMPORTANT: order is per-step, not per-branch.
        # corner.N must complete BEFORE lvf.N / em.N (cross-branch dep).
        for step in [1, 2, 3]:
            _step(f"step {step} — corner ({len(CORNER_PVTRCS)} PVTRCs)",
                  lambda s=step: materialize_partitions(
                      _asset(f"corner_step{s}"), CORNER_PVTRCS))
            _step(f"step {step} — lvf ({len(LVF_PVTRCS)} PVTRCs, deps corner.{step})",
                  lambda s=step: materialize_partitions(
                      _asset(f"lvf_step{s}"), LVF_PVTRCS))
            _step(f"step {step} — em ({len(EM_PVTRCS)} PVTRCs, deps corner.{step})",
                  lambda s=step: materialize_partitions(
                      _asset(f"em_step{s}"), EM_PVTRCS))

        _step("step 4 — cross_branch_signoff",
              lambda: materialize([_asset("cross_branch_signoff")],
                                  resources=defs.resources))

    except AssertionError as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== ALL STEPS PASS — total {time.time() - overall:.1f}s ===")

    out = Path("/tmp/dagster-10-flow")
    counts = {
        "cells.json": 1 if (out / "cells.json").exists() else 0,
    }
    for branch, pvtrcs in PVTRCS_PER_BRANCH.items():
        for s in [1, 2, 3]:
            d = out / branch / f"step{s}"
            counts[f"{branch}/step{s}"] = len(list(d.glob("*.out")))
    counts["cross_branch_signoff"] = (
        1 if (out / "cross_branch_signoff.tar").exists() else 0
    )

    print("\nartifact counts:")
    expected = {
        "cells.json": 1,
        "corner/step1": 3, "corner/step2": 3, "corner/step3": 3,
        "lvf/step1": 1, "lvf/step2": 1, "lvf/step3": 1,
        "em/step1": 2, "em/step2": 2, "em/step3": 2,
        "cross_branch_signoff": 1,
    }
    for k in expected:
        ok = "OK" if counts[k] == expected[k] else "MISMATCH"
        print(f"  {k}: got {counts[k]}, expected {expected[k]}  {ok}")
    assert counts == expected, f"mismatch: {counts} != {expected}"
    print("\nALL ARTIFACT COUNTS MATCH")
