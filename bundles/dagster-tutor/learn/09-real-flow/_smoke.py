"""End-to-end smoke test driver. Run after `dagster definitions
validate` passes, to actually exercise the chain.

Usage:
    cd learn/09-real-flow
    python -m _smoke

Materializes every partition through every asset in dependency
order. ~2-3 minutes total (subprocess overhead on 72 LPE +
18 char + filesystem fan-in).
"""

import os
import shutil
import sys
import time
from pathlib import Path

# Required before importing pipelines (which creates output dirs)
os.environ.setdefault("DAGSTER_HOME", "/tmp/dagster-09-test-home")
Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)

# Clean previous outputs for a fresh run
shutil.rmtree("/tmp/dagster-09-flow", ignore_errors=True)

from dagster import MultiPartitionKey, materialize  # noqa: E402

from pipelines.asset import (  # noqa: E402
    cell_list,
    corner_setup,
    lpe,
    netlist_gen,
    liberate_run,
    liberty_aggregate,
    signoff_lib,
)
from pipelines.asset import defs  # noqa: E402  (resources)
from pipelines.partitions import CELLS, CORNERS, VOLT_TEMPS  # noqa: E402


resources = defs.resources


def _step(label: str, fn):
    print(f"\n>>> {label}")
    t0 = time.time()
    fn()
    print(f"    done in {time.time() - t0:.1f}s")


def run_cell_list():
    r = materialize([cell_list], resources=resources)
    assert r.success, "cell_list failed"


def run_lpe_all():
    for corner in CORNERS:
        for vt in VOLT_TEMPS:
            for cell in CELLS:
                key = MultiPartitionKey({
                    "corner": corner,
                    "vt_cell": f"{vt}__{cell}",
                })
                r = materialize(
                    [lpe],
                    partition_key=key,
                    resources=resources,
                    selection=[lpe],
                )
                assert r.success, f"lpe {key} failed"


def run_corner_chain(asset_obj, label: str):
    for corner in CORNERS:
        r = materialize([asset_obj], partition_key=corner, resources=resources)
        assert r.success, f"{label} {corner} failed"


def run_liberate_all():
    for corner in CORNERS:
        for vt in VOLT_TEMPS:
            key = MultiPartitionKey({"corner": corner, "volt_temp": vt})
            r = materialize(
                [liberate_run],
                partition_key=key,
                resources=resources,
                selection=[liberate_run],
            )
            assert r.success, f"liberate {key} failed"


def run_signoff():
    r = materialize([signoff_lib], resources=resources)
    assert r.success, "signoff failed"


if __name__ == "__main__":
    overall_start = time.time()
    try:
        _step("step 0 — cell_list (Perl)", run_cell_list)
        _step("step 1 — lpe (Perl, 72 partitions)", run_lpe_all)
        _step("step 2 — corner_setup (Python, 3 partitions)",
              lambda: run_corner_chain(corner_setup, "corner_setup"))
        _step("step 3 — netlist_gen (Python, 3 partitions)",
              lambda: run_corner_chain(netlist_gen, "netlist_gen"))
        _step("step 4 — liberate_run (Pipes+TCL, 18 partitions)", run_liberate_all)
        _step("step 5 — liberty_aggregate (Python, 3 partitions)",
              lambda: run_corner_chain(liberty_aggregate, "liberty_aggregate"))
        _step("step 6 — signoff_lib (Perl)", run_signoff)
    except AssertionError as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - overall_start
    print(f"\n=== ALL STEPS PASS — total {elapsed:.1f}s ===")

    # Verify output artifacts
    out = Path("/tmp/dagster-09-flow")
    counts = {
        "cell_list": len(list((out / "cell_list").glob("*.json"))),
        "lpe": len(list((out / "lpe").glob("*.spef"))),
        "corner_setup": len(list((out / "corner_setup").glob("*.setup"))),
        "netlist_gen": len(list((out / "netlist_gen").glob("*.netlist"))),
        "liberate": len(list((out / "liberate").glob("*.lib"))),
        "aggregate": len(list((out / "liberty_aggregate").glob("*.lib"))),
        "signoff": len(list((out / "signoff").glob("*"))),
    }
    print("\nartifact counts:")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    expected = {
        "cell_list": 1, "lpe": 72, "corner_setup": 3,
        "netlist_gen": 3, "liberate": 18, "aggregate": 3, "signoff": 1,
    }
    assert counts == expected, f"artifact mismatch: {counts} != {expected}"
    print("\nartifact counts MATCH expected")
