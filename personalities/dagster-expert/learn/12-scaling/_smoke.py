"""End-to-end smoke driver for lesson 12 — both layouts.

Validates and materializes both `high_cardinality/` and `compact/`,
printing asset counts side-by-side at the end so the
cardinality difference is visible in test output.
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

LESSON_ROOT = Path(__file__).parent
HIGH_CARD = LESSON_ROOT / "high_cardinality"
COMPACT = LESSON_ROOT / "compact"

# Clean previous outputs
shutil.rmtree("/tmp/dagster-12-high-cardinality", ignore_errors=True)
shutil.rmtree("/tmp/dagster-12-compact", ignore_errors=True)
os.environ.setdefault("DAGSTER_HOME", "/tmp/dagster-12-test-home")
Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)


def _validate(workdir: Path, name: str) -> int:
    print(f">>> validate {name}")
    r = subprocess.run(
        ["dagster", "definitions", "validate", "-m", "pipelines"],
        cwd=workdir, capture_output=True, text=True,
    )
    ok = "All code locations passed validation" in r.stdout + r.stderr
    print(f"    {'PASS' if ok else 'FAIL'}")
    if not ok:
        print((r.stdout + r.stderr)[-500:])
    return 0 if ok else 1


def _materialize_compact():
    print(">>> materialize compact (5 step assets × 24 partitions = 120 mats)")
    sys.path.insert(0, str(COMPACT))
    try:
        from dagster import MultiPartitionKey, materialize  # noqa: E402
        from pipelines.asset import (  # noqa: E402
            BRANCHES, LIBRARIES, PVTRCS, STEP_TYPES,
            cell_list, defs,
        )

        t0 = time.time()
        r = materialize([cell_list], resources=defs.resources)
        assert r.success
        for step_idx, st in enumerate(STEP_TYPES):
            step_asset = next(a for a in defs.assets if a.key.path == [st])
            for lib in LIBRARIES:
                for br in BRANCHES:
                    for pvtrc in PVTRCS:
                        key = MultiPartitionKey({
                            "lib_branch": f"{lib}__{br}",
                            "pvtrc": pvtrc,
                        })
                        r = materialize(
                            [step_asset], partition_key=key,
                            resources=defs.resources, selection=[step_asset],
                        )
                        assert r.success, f"{st}@{key} failed"
        print(f"    PASS  ({time.time() - t0:.1f}s)")
    finally:
        sys.path.pop(0)
        for mod in list(sys.modules):
            if mod.startswith("pipelines"):
                del sys.modules[mod]


def _materialize_high_cardinality():
    print(">>> materialize high_cardinality (40 step assets × 3 partitions)")
    sys.path.insert(0, str(HIGH_CARD))
    try:
        from dagster import AssetKey, materialize  # noqa: E402
        from pipelines.asset import (  # noqa: E402
            BRANCHES, LIBRARIES, PVTRCS, STEP_TYPES, defs,
        )

        t0 = time.time()
        for a in defs.assets:
            if a.key.path == ["cell_list"]:
                r = materialize([a], resources=defs.resources)
                assert r.success
                break

        for lib in LIBRARIES:
            for br in BRANCHES:
                for i, st in enumerate(STEP_TYPES):
                    asset_obj = next(
                        a for a in defs.assets
                        if a.key.path == [lib, br, st]
                    )
                    for pvtrc in PVTRCS:
                        r = materialize(
                            [asset_obj], partition_key=pvtrc,
                            resources=defs.resources, selection=[asset_obj],
                        )
                        assert r.success, (
                            f"{lib}/{br}/{st}@{pvtrc} failed"
                        )
        print(f"    PASS  ({time.time() - t0:.1f}s)")
    finally:
        sys.path.pop(0)
        for mod in list(sys.modules):
            if mod.startswith("pipelines"):
                del sys.modules[mod]


def _count_assets():
    print("\n=== asset counts (Dagster instance perspective) ===")
    for name, workdir in [("compact", COMPACT),
                           ("high_cardinality", HIGH_CARD)]:
        sys.path.insert(0, str(workdir))
        try:
            for mod in list(sys.modules):
                if mod.startswith("pipelines"):
                    del sys.modules[mod]
            from pipelines.asset import defs  # noqa: E402
            n = len(defs.assets)
            print(f"  {name:20s} {n} @asset declarations")
        finally:
            sys.path.pop(0)


if __name__ == "__main__":
    overall = time.time()
    fails = 0
    fails += _validate(COMPACT, "compact")
    fails += _validate(HIGH_CARD, "high_cardinality")
    if fails:
        sys.exit(1)

    _materialize_compact()
    _materialize_high_cardinality()
    _count_assets()
    print(f"\n=== all checks PASS — total {time.time() - overall:.1f}s ===")
    print(
        "\nFor production-scale projection (6 × 50 × 15 = 4500 vs 15 assets):\n"
        "    python -m cardinality_calc"
    )
