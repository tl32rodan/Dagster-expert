---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-12T14:00:00+0800
tags: [lesson-12, scaling, calibration, sqlite, cardinality, level-3]
---
# Lesson 12 Level-3 framing is too eager — single SQLite location reaches further than I claimed

## Observation

Lesson 12 (`learn/12-scaling/README.md`) presents 5 scaling levels and
proposes "per-library code location" (Level 3) at 5,000–75,000 assets
or "100 libraries" scale. Brian's `demo/scale-lib/` production
calibration says otherwise.

`cardinality_calc.py` output for the realistic projection:

```
DEMO         1 lib ×  46 branches × 21 steps ≈  1.1k partition records
PRODUCTION   1 lib ×  64 branches × 21 steps ≈  1.5k partition records
FUTURE 10×   1 lib × 460 branches × 21 steps ≈ 10.6k partition records
```

**Even the 10× future projection fits SQLite comfortably.** Code-location
split (Level 3 of my lesson 12) is for *very* large scale — 100+ libraries
× this much branch coverage — not the "100 libraries" round-number I
landed on. My Level-3 trigger threshold should be raised by ~10×.

## Why my lesson 12 framing is misleading

The Level 3 section says:

> Level 3: large (5,000–75,000 assets — 100 libraries)
> Compact alone keeps asset count low (15), BUT partition record
> count balloons (1.5M). Single-location queries on 1.5M rows
> get slow.

This is over-eager. With `compact` layout + the actual partition-row
math:

| Layout | Asset count | Partition rows (1 lib × 64 branches × 21 steps) |
|---|---|---|
| `high_cardinality` per (lib×branch×step) | 64 × 21 = 1,344 (per lib) | × 20 PVTRC = 26.8k |
| `compact` per step | 21 (per lib) | × (64 × 20) = 26.8k |

For 6 libs: compact = 21 assets, partition rows = ~160k. Still SQLite-OK.
For 64 libs: compact = 21 assets, partition rows = ~1.7M. Now Postgres
needed but **still single-location** unless query latency or failure-
isolation specifically demands split.

The right framing for Level 3 is **"when query latency on Postgres OR
failure-isolation needs make single-location untenable"** — not a fixed
asset / partition count threshold.

## What the lesson should say (revised)

| Level | Trigger | Action |
|---|---|---|
| 1 small | <1k partition records | any layout + default SQLite |
| 2 medium | 1k–1M partition records OR >999 assets | **compact layout + Postgres** |
| 3 large | Postgres query latency unacceptable, OR failure-isolation per library required, OR per-team deploy separation | **per-library code location** |
| 4 very large | Multiple Dagster instances needed (multi-tenant strict separation) | **per-team Dagster instance** |
| 5 extreme | Tier-1 cardinality > 1M (rare; usually means leaf-fan-out should be Tier-2 instead) | **scrap Dagster-as-orchestrator** OR re-evaluate tier cut |

## Underlying mistake I made

I confused **asset count** (which trips SQLite's `?`-placeholder limit) with
**partition record count** (which is a query-throughput concern, not a
placeholder concern). Compact layout decouples these:

- 21 assets × 100 libraries × 64 branches × 20 PVTRC = 21 asset records,
  2.7M partition records.
- 21 asset records is comfortably under both SQLite limits.
- 2.7M partition records is a Postgres workload but still single-location.

The Level-3 trigger I wrote conflated them and proposed code-location
split prematurely.

## Curator action

1. Edit `learn/12-scaling/README.md` § "Scaling levels":
   - Replace the asset-count thresholds with **partition-row-count +
     query-latency + isolation-need** triggers
   - Cross-reference `demo/scale-lib/`'s cardinality_calc output as the
     ground truth (already production-validated)
   - Note that "100 libraries" alone doesn't trigger Level 3 if cardinality
     comes from branches/steps, not libraries

2. Add a note at the top of lesson 12: "for production-shape reference,
   see `demo/scale-lib/`" — it's the calibration anchor.

3. Update `cardinality_calc.py` to print which level applies, using the
   revised trigger criteria.

## References

- `personalities/dagster-expert/learn/12-scaling/README.md` (the file to edit)
- `personalities/dagster-expert/demo/scale-lib/README.md` § Cardinality
- `personalities/dagster-expert/memory/understanding/scale-lib-demo.md`
