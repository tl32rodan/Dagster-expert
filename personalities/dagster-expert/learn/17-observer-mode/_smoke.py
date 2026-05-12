"""Smoke driver for lesson 17 — observer-mode prototype.

Simulates a mock AP run sequence by touching files, then
materializes the Tier-1 recorder assets manually (since we have
no daemon in a smoke run). Verifies the data_version moves when
the touch file moves.
"""

import json
import os
import shutil
import sys
import time
from pathlib import Path

os.environ.setdefault("DAGSTER_HOME", "/tmp/dagster-17-test-home")
Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)
shutil.rmtree("/tmp/dagster-17-mock-ap", ignore_errors=True)
shutil.rmtree("/tmp/dagster-17-tier1-records", ignore_errors=True)

# Make sure cwd has pipelines/ on path
sys.path.insert(0, str(Path(__file__).parent))

from dagster import AssetKey, materialize  # noqa: E402

from pipelines.asset import (  # noqa: E402
    AP_ROOT,
    AP_STEPS,
    TIER1_RECORDS,
    _ap_output_dir,
    _ap_touch_file,
    _tier1_recorders,
    defs,
)


def _mock_ap_run(step: str, payload: str):
    """Simulate AP completing a step: write to output dir + touch the .done file."""
    out = _ap_output_dir(step)
    out.mkdir(parents=True, exist_ok=True)
    (out / "result.txt").write_text(payload)
    (out / "log.txt").write_text(f"AP {step} completed at {time.time()}\n")
    touch = _ap_touch_file(step)
    touch.parent.mkdir(parents=True, exist_ok=True)
    touch.touch()


def _materialize_tier1(step: str):
    asset_obj = next(a for a in defs.assets
                     if a.key.path == [f"tier1_{step}_record"])
    r = materialize([asset_obj], resources=defs.resources)
    assert r.success, f"tier1_{step}_record materialization failed"


def _latest_record(step: str) -> dict:
    files = sorted(TIER1_RECORDS.glob(f"{step}__*.json"))
    return json.loads(files[-1].read_text())


if __name__ == "__main__":
    overall = time.time()

    print(">>> Round 1: AP completes step0 + step1 + step2")
    for s, p in [("step0", "first run"), ("step1", "from step0"),
                  ("step2", "from step1")]:
        _mock_ap_run(s, p)
    print("    AP touch files written + output folders populated")

    print(">>> Tier 1 records each step (observer mode)")
    for s in AP_STEPS:
        _materialize_tier1(s)
        rec = _latest_record(s)
        print(f"    {s}: digest={rec['folder_digest'][:12]}... files={rec['file_count']}")

    print("\n>>> Round 2: AP re-runs step1 with new payload")
    time.sleep(1.1)  # ensure mtime moves at second granularity
    _mock_ap_run("step1", "second run — content changed")
    print("    AP touch + output rewritten")

    print(">>> Tier 1 re-records step1")
    _materialize_tier1("step1")
    r1 = _latest_record("step1")
    r0 = sorted(TIER1_RECORDS.glob("step1__*.json"))[0]
    r0_data = json.loads(Path(r0).read_text())
    print(f"    step1 round 1 digest: {r0_data['folder_digest'][:12]}...")
    print(f"    step1 round 2 digest: {r1['folder_digest'][:12]}...")
    assert r0_data["folder_digest"] != r1["folder_digest"], (
        "step1 digest didn't change between runs — Tier 1 missed the AP update"
    )
    print("    DIGESTS DIFFER — Tier 1 caught the AP rerun")

    print(f"\n=== PASS  ({time.time() - overall:.1f}s) ===")
    print(f"records written to: {TIER1_RECORDS}")
    print(f"AP scratch dir: {AP_ROOT}")
