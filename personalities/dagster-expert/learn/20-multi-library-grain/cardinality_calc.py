"""Cardinality math for lesson 20 — run BEFORE believing the design fits.

Prints the asset count, partition-record count, and the breakdown by
StepKind. Loads ``pipelines/branches.py``, ``pipelines/steps.py``, and
``pipelines/libraries.py`` directly by file path (via ``importlib.util``)
so it works even on a workstation that hasn't activated the venv —
the data modules don't import Dagster.

Run:
  python3 cardinality_calc.py
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load(modname: str, relpath: str):
    """Load a single module by file path, bypassing the ``pipelines/`` package
    init (which eagerly imports Dagster for the workspace loader's sake)."""
    full = Path(__file__).resolve().parent / relpath
    spec = importlib.util.spec_from_file_location(modname, full)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load {modname} from {full}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


_branches = _load("_l20_branches", "pipelines/branches.py")
_steps = _load("_l20_steps", "pipelines/steps.py")
_libraries = _load("_l20_libraries", "pipelines/libraries.py")


def main() -> None:
    n_lib = len(_libraries.LIBRARIES)
    n_branch = len(_branches.all_branches())
    n_root = len(_branches.roots())
    n_step = len(_steps.STEPS)

    n_root_only_steps = (
        len(_steps.setup_steps()) + len(_steps.kits())
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
    print(f"  setup_root_only steps         : {len(_steps.setup_steps())}")
    print(f"  extraction steps              : "
          f"{sum(1 for s in _steps.STEPS if s.kind is _steps.StepKind.EXTRACTION)}")
    print(f"  char steps                    : "
          f"{sum(1 for s in _steps.STEPS if s.kind is _steps.StepKind.CHAR)}")
    print(f"  kit_root_only steps           : {len(_steps.kits())}")
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
