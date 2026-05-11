"""Smoke driver for lesson 13 — LSF integration with mock bsub."""

import os
import shutil
import sys
import time
from pathlib import Path

LESSON_ROOT = Path(__file__).parent
MOCK_LSF = LESSON_ROOT / "scripts" / "mock_lsf"

# Prepend mock LSF to PATH so `bsub` resolves to our shim
os.environ["PATH"] = f"{MOCK_LSF}:{os.environ.get('PATH', '')}"
os.environ.setdefault("DAGSTER_HOME", "/tmp/dagster-13-test-home")
Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)
shutil.rmtree("/tmp/dagster-13-lsf", ignore_errors=True)

# Make scripts executable
for p in [MOCK_LSF / "bsub",
          LESSON_ROOT / "scripts" / "python" / "lsf_submit.py",
          LESSON_ROOT / "scripts" / "python" / "char_inner.py"]:
    os.chmod(p, 0o755)

from dagster import MultiPartitionKey, materialize  # noqa: E402

from pipelines.asset import char_via_lsf, defs  # noqa: E402
from pipelines.asset import CORNERS, VTS  # noqa: E402


if __name__ == "__main__":
    overall = time.time()
    print(f"PATH front: {os.environ['PATH'].split(':')[0]}")

    for corner in CORNERS:
        for vt in VTS:
            key = MultiPartitionKey({"corner": corner, "volt_temp": vt})
            print(f">>> materialize char_via_lsf @ {key}")
            r = materialize(
                [char_via_lsf], partition_key=key,
                resources=defs.resources, selection=[char_via_lsf],
            )
            assert r.success, f"failed @ {key}"
            print(f"    ok")

    print(f"\n=== ALL PASS — {time.time() - overall:.1f}s ===")

    libs = list(Path("/tmp/dagster-13-lsf/libs").glob("*.lib"))
    logs = list(Path("/tmp/dagster-13-lsf/logs").glob("*.out"))
    expected = len(CORNERS) * len(VTS)
    print(f"\nartifacts: {len(libs)} .lib (expect {expected}), "
          f"{len(logs)} .out logs")
    assert len(libs) == expected
    print("OK")
