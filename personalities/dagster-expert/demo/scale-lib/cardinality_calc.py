"""Cardinality predictor for scale-lib.

Prints partition-record totals for three scenarios:
  1. demo as-shipped         (1 library × 46 branches)
  2. production (user said)  (1 library × 64 branches)
  3. future 10× growth       (1 library × 460 branches)

Tier-1 cardinality is independent of PVT count (PVT is script-internal),
so it does not appear in this math. See CONTRACT.md.

Run:  python3 cardinality_calc.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipelines.spec.step_taxonomy import STEPS, StepKind  # noqa: E402


def _per_step_partitions(num_branches: int, num_roots: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    for s in STEPS:
        if s.kind in (StepKind.SETUP_ROOT_ONLY, StepKind.KIT_ROOT_ONLY):
            counts[s.name] = num_roots
        else:
            counts[s.name] = num_branches
    return counts


def predict(label: str, num_branches: int, num_roots: int = 1, num_libraries: int = 1):
    per_step = _per_step_partitions(num_branches, num_roots)
    total_partition_records = sum(per_step.values()) * num_libraries
    print(f"=== {label} ===")
    print(f"  libraries:               {num_libraries}")
    print(f"  branches:                {num_branches}")
    print(f"  roots:                   {num_roots}")
    print(f"  step assets per library: {len(STEPS)}")
    print(f"  total asset declarations:{len(STEPS) * num_libraries}")
    print(f"  total partition records: {total_partition_records}")
    print()
    print("  per-step partition counts:")
    for name, c in per_step.items():
        print(f"    {name:<16} {c}")
    print()


def main():
    predict("DEMO (as-shipped)",            num_branches=46)
    predict("PRODUCTION (user estimate)",   num_branches=64)
    predict("FUTURE 10× (stress)",          num_branches=460)
    print("Note: PVT and cell are Tier-2 (script-internal) — not counted")
    print("above. See CONTRACT.md §LSF swap point for how the script")
    print("fans out internally without affecting Tier-1 cardinality.")


if __name__ == "__main__":
    main()
