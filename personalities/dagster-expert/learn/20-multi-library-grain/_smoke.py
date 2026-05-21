"""End-to-end smoke for lesson 20.

What it does:
  - Loads the 2,100-asset Definitions (timed).
  - Picks 3 libraries × a small slice of branches × the gate + chain
    (step0, step1, step2..step5, step6, then meta kit).
  - Materializes that slice via in-process ``materialize`` (no dagit).
  - Asserts every materialization succeeded.

What it does NOT do:
  - Touch all 100 libraries (would take too long for smoke).
  - Verify UI / lineage shape (run ``dagster dev`` for that — see README).

Run (tcsh):
  setenv DAGSTER_HOME ~/.dagster-tutor/20-multi-library-grain
  mkdir -p $DAGSTER_HOME
  python3 _smoke.py

(bash: ``export DAGSTER_HOME=~/.dagster-tutor/20-multi-library-grain``)
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path


_LESSON_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_LESSON_ROOT))


def _require_dagster_home() -> None:
    if not os.environ.get("DAGSTER_HOME"):
        print(
            "ERROR: $DAGSTER_HOME not set. Per-lesson isolation is mandatory.\n"
            "  tcsh: setenv DAGSTER_HOME ~/.dagster-tutor/20-multi-library-grain\n"
            "  bash: export DAGSTER_HOME=~/.dagster-tutor/20-multi-library-grain",
            file=sys.stderr,
        )
        sys.exit(2)
    Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)


def _time(label: str, fn):
    t0 = time.time()
    fn()
    print(f"  {label:<55} ok ({time.time() - t0:.2f}s)")


def main() -> int:
    _require_dagster_home()

    # Defer Dagster import until after DAGSTER_HOME is set.
    from dagster import AssetKey, materialize  # noqa: E402

    from pipelines import defs  # noqa: E402
    from pipelines.libraries import LIBRARIES  # noqa: E402

    print(">>> loading Definitions (this builds 2,100 assets)")
    t0 = time.time()
    assets_by_key = {a.key: a for a in defs.assets}
    print(f"    loaded {len(assets_by_key)} asset defs in {time.time() - t0:.1f}s")

    def _materialize_one(asset_obj, partition_key: str) -> None:
        result = materialize(
            [asset_obj],
            partition_key=partition_key,
            raise_on_error=True,
        )
        assert result.success, f"{asset_obj.key.path} @ {partition_key} failed"

    libs = LIBRARIES[:3]  # first 3 libraries: svt_p1_h6_075/080/090
    branches_to_test = ["corner", "em", "ht", "lvf", "lvf_ht"]
    chain_path = ["step0", "step1", "step2", "step3", "step4", "step5", "step6"]
    kit_path = ["meta"]

    def _get(library: str, step_name: str):
        key = AssetKey([library, step_name])
        if key not in assets_by_key:
            raise KeyError(key.path)
        return assets_by_key[key]

    print()
    print(f">>> materializing {len(libs)} libs × subset of branches × "
          f"{len(chain_path) + len(kit_path)} steps")

    for lib in libs:
        print(f"\n--- library: {lib} ---")
        # step0 (root only)
        _time(
            f"{lib}/step0[corner]",
            lambda l=lib: _materialize_one(_get(l, "step0"), "corner"),
        )
        # step1 (all branches in our subset)
        for br in branches_to_test:
            _time(
                f"{lib}/step1[{br}]",
                lambda l=lib, b=br: _materialize_one(_get(l, "step1"), b),
            )
        # step2..step6 chain (each in subset of branches)
        for st in ["step2", "step3", "step4", "step5", "step6"]:
            for br in branches_to_test:
                _time(
                    f"{lib}/{st}[{br}]",
                    lambda l=lib, s=st, b=br: _materialize_one(_get(l, s), b),
                )
        # meta kit (root only — gates on step6[corner])
        _time(
            f"{lib}/meta[corner]",
            lambda l=lib: _materialize_one(_get(l, "meta"), "corner"),
        )

    total_runs = (
        len(libs)
        * (1 + len(branches_to_test) + 5 * len(branches_to_test) + 1)
    )
    print()
    print("=" * 64)
    print(f"SMOKE PASS — {total_runs} successful materializations across "
          f"{len(libs)} libraries.")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
