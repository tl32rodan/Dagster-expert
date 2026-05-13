"""Cardinality calculator.

Given your real production parameters, estimates:
- Asset count under "high cardinality" (one @asset per
  (library, branch, step_type) tuple) — Lesson 11 / current style
- Asset count under "compact" (one @asset per step_type, with
  (library_branch × pvtrc) MultiPartitions)
- Partition record count (same for both — only the asset count
  differs)
- SQLite headroom assessment

Usage:
    python -m cardinality_calc                # uses Brian's production numbers
    python -m cardinality_calc 6 50 15 20     # libs, branches, step_types, pvtrcs
"""

import sys


def estimate(libs: int, branches: int, step_types: int, pvtrcs: int):
    print(f"Inputs:")
    print(f"  libraries     = {libs}")
    print(f"  branches      = {branches}")
    print(f"  step_types    = {step_types}")
    print(f"  pvtrcs        = {pvtrcs}")
    print()

    high_card_assets = libs * branches * step_types
    high_card_partitions = high_card_assets * pvtrcs

    # In compact: one asset per step_type; partitions are
    # (lib_branch × pvtrc).
    compact_assets = step_types
    compact_partitions = compact_assets * (libs * branches) * pvtrcs

    print(f"=== High-cardinality (one @asset per lib×branch×step) ===")
    print(f"  asset declarations  = {high_card_assets:,}")
    print(f"  partition records   = {high_card_partitions:,}")
    print()
    print(f"=== Compact (one @asset per step, MultiPartitions) ===")
    print(f"  asset declarations  = {compact_assets:,}")
    print(f"  partition records   = {compact_partitions:,}")
    print(f"  reduction factor    = {high_card_assets // compact_assets:,}× fewer assets")
    print()

    # SQLite assessment
    SQLITE_OLD = 999       # < SQLite 3.32
    SQLITE_NEW = 32766     # SQLite 3.32+
    print(f"=== SQLite ?-placeholder assessment ===")
    print(f"  default limits: old={SQLITE_OLD:,}, new={SQLITE_NEW:,}")
    print()

    for name, n in [("high-card asset count", high_card_assets),
                     ("compact asset count",   compact_assets)]:
        old_ok = "OK" if n < SQLITE_OLD else f"OVER (×{n / SQLITE_OLD:.1f})"
        new_ok = "OK" if n < SQLITE_NEW else f"OVER (×{n / SQLITE_NEW:.1f})"
        print(f"  {name}: {n:,}  → old: {old_ok}, new: {new_ok}")
    print()

    # Recommendation
    print(f"=== Recommendation ===")
    if high_card_assets > SQLITE_OLD:
        print(f"  high-card layout will trip SQLite (old). Either:")
        print(f"    - Switch to Postgres (no ?-limit at this scale), OR")
        print(f"    - Refactor to compact layout ({compact_assets} assets — safe under any SQLite)")
    if compact_assets < SQLITE_NEW and high_card_assets > SQLITE_NEW:
        print(f"  Compact layout fits under new SQLite without Postgres.")
    if high_card_assets > SQLITE_NEW:
        print(f"  Both layouts overflow new SQLite — must use Postgres.")
    print(f"  Best practice for your scale: Postgres + compact = both lanes covered.")
    print()

    # Scaling level (revised triggers per scale-lib calibration)
    print(f"=== Scaling level (compact + Postgres assumed) ===")
    if compact_partitions < 1_000:
        level, action = 1, "any layout + SQLite is fine"
    elif compact_partitions < 1_000_000:
        level, action = 2, "compact layout + Postgres (most production EDA flows)"
    elif compact_partitions < 10_000_000:
        level, action = 3, ("per-library code locations IF query latency / "
                            "failure-isolation / deploy-cadence demands it; "
                            "otherwise single-location compact + Postgres still works")
    else:
        level, action = 5, ("re-evaluate tier cut — Tier-1 cardinality this "
                            "high usually means leaf work should be in "
                            "Tier-2 (LSF/Slurm) not Dagster partitions")
    print(f"  Level {level}: {action}")


if __name__ == "__main__":
    if len(sys.argv) == 5:
        estimate(*[int(x) for x in sys.argv[1:]])
    elif len(sys.argv) == 1:
        # Brian's production numbers (May 2026):
        print("(no args — using Brian's reported production numbers)\n")
        estimate(libs=6, branches=50, step_types=15, pvtrcs=20)
    else:
        print(__doc__)
        sys.exit(1)
