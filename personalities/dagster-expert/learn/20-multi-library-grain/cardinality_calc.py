"""Cardinality math for lesson 20 — run BEFORE believing the design fits.

Prints the asset count, partition-record count, and the breakdown by
StepKind. Independent of Dagster (no imports from the framework) so it
works on a workstation that hasn't activated the venv yet.

Run:
  python3 cardinality_calc.py
"""
from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))


from pipelines.branches import all_branches, roots  # noqa: E402
from pipelines.libraries import LIBRARIES  # noqa: E402
from pipelines.steps import (  # noqa: E402
    STEPS,
    StepKind,
    kits,
    setup_steps,
)


def main() -> None:
    n_lib = len(LIBRARIES)
    n_branch = len(all_branches())
    n_root = len(roots())
    n_step = len(STEPS)

    n_root_only_steps = (
        len(setup_steps()) + len(kits())
    )
    n_all_branch_steps = n_step - n_root_only_steps

    n_assets = n_lib * n_step
    n_root_only_partitions = n_lib * n_root_only_steps * n_root
    n_all_branch_partitions = n_lib * n_all_branch_steps * n_branch
    n_partitions = n_root_only_partitions + n_all_branch_partitions

    print("=" * 60)
    print("Lesson 20 — multi-library grain cardinality")
    print("=" * 60)
    print()
    print(f"Libraries (= UI groups)         : {n_lib}")
    print(f"Steps per library (= assets/lib): {n_step}")
    print(f"  setup_root_only steps         : {len(setup_steps())}")
    print(f"  extraction steps              : "
          f"{sum(1 for s in STEPS if s.kind is StepKind.EXTRACTION)}")
    print(f"  char steps                    : "
          f"{sum(1 for s in STEPS if s.kind is StepKind.CHAR)}")
    print(f"  kit_root_only steps           : {len(kits())}")
    print(f"Branches                        : {n_branch} (root: {n_root})")
    print()
    print(f"Total assets                    : {n_assets:,}")
    print(f"Total partition records         : {n_partitions:,}")
    print(f"  from root-only steps          : {n_root_only_partitions:,} "
          f"({n_lib} × {n_root_only_steps} × {n_root})")
    print(f"  from all-branch steps         : {n_all_branch_partitions:,} "
          f"({n_lib} × {n_all_branch_steps} × {n_branch})")
    print()
    print("Storage guidance:")
    print(f"  SQLite handles ~100k partition records comfortably. "
          f"{n_partitions:,} fits.")
    print(f"  Postgres recommended if concurrent backfills push past "
          f"100k or you cross 5k partitions per asset.")


if __name__ == "__main__":
    main()
