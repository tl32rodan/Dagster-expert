# lab12 · scaling beyond SQLite

**Time**: 90 min · **Prerequisites**: lessons 09–11

## Why this lab exists

Brian hit a real production-scale wall on 2026-05-11:

> "讓他根據內部 AP 最近一次 production 的一套 library 來做真實場景模擬:
> 6 套 library 對應不同 vt: svt, lvt, lvtll, ulvt, ulvtll, elvt. 總共
> 有~50 個 branch, 以及 20+ 個 step/kit ... 發現這樣展開後 (step/kit,
> branch) 量太大, sqlite 報 too many argument."

Production scale (his actual numbers):
- 6 libraries × 50 branches × 15 step-types × 20 PVTRC each
- = 4,500 asset declarations × 20 partitions = **90,000 partition records**

SQLite trips its `?`-placeholder ceiling. Dagster's bulk queries
batch `WHERE asset_key IN (?, ?, ?, ...)` and overflow.

## SQLite `SQLITE_MAX_VARIABLE_NUMBER` — the real limit

| SQLite version | `?` placeholder limit |
|---|---|
| `< 3.32` (pre-2020) | **999** |
| `3.32+` (2020+) | **32,766** |

Dagster's instance DB queries scale primarily by **asset count**
(not partition count). Brian's 4500 assets trips the old limit
4.5×, fits new SQLite — until the count grows.

Verify your SQLite version: `sqlite3 --version`.

## Two-axis solution: cardinality refactor + database swap

These are **complementary, not alternatives**:

| Lever | What it fixes | When |
|---|---|---|
| **Compact asset layout** (this lesson) | Reduces asset count by `N_branch × N_lib_per_step` | Always, regardless of DB |
| **Switch to Postgres** (POSTGRES_MIGRATION.md) | Removes `?`-placeholder limit | When you have >999 assets even after refactor |
| **Per-library code locations** | Distributes load across N processes | 100+ libraries |
| **Per-team Dagster instance** | Fully separate deployments | 1000+ libraries / multi-tenant |

Brian's recommended next steps: **(1) Postgres** (he already has
it deployed) + **(2) compact refactor** (lesson 11 → this lesson's
`compact/` shape).

## Lesson layout

```
12-scaling/
├── README.md                ← this file
├── POSTGRES_MIGRATION.md    ← step-by-step SQLite → Postgres
├── cardinality_calc.py      ← input (libs, branches, steps, pvtrcs)
│                              → output (asset count, partition count,
│                                SQLite headroom, recommendation)
├── compact/                 ← ⭐ the recommended layout
│   └── pipelines/asset.py   ← one @asset per step_type;
│                              (lib_branch × pvtrc) partitions
├── high_cardinality/        ← lesson-11 style for comparison
│   └── pipelines/asset.py   ← one @asset per (lib, branch, step);
│                              pvtrc partitions only
├── multi_location/          ← Level 3: per-library code location
│   ├── workspace.yaml
│   ├── svt/pipelines/asset.py
│   ├── lvt/pipelines/asset.py
│   └── ulvt/pipelines/asset.py
└── _smoke.py                ← validates + materializes both styles
```

## Run the comparison

```bash
source ~/dagster-venv/bin/activate
cd ~/projects/.../learn/12-scaling
python -m _smoke
```

~60s. Output shows:

```
=== asset counts (Dagster instance perspective) ===
  compact              6 @asset declarations
  high_cardinality     41 @asset declarations
```

At this small demo scale (2 libs × 4 branches × 5 steps), the
ratio is ~7×. At Brian's production scale (6 × 50 × 15), the
ratio is **300×** — the math is in `cardinality_calc.py`.

## Compact layout — the refactor

**Old shape (lesson 11)**: one `@asset` per `(library, branch, step_type)`.

```python
# 4500 @asset declarations
@asset(key_prefix=["svt"], name="corner_step1", ...)
def svt_corner_step1(...): ...

@asset(key_prefix=["svt"], name="corner_step2", ...)
def svt_corner_step2(...): ...

# ... 4498 more
```

**New shape (compact)**: one `@asset` per `step_type`, with
`(library_branch × pvtrc)` as `MultiPartitionsDefinition`.

```python
# 15 @asset declarations
lib_branch_pvtrc = MultiPartitionsDefinition({
    "lib_branch": StaticPartitionsDefinition(LIB_BRANCH_KEYS),  # N × M
    "pvtrc":      StaticPartitionsDefinition(PVTRCS),           # P
})

@asset(name="corner_step1", partitions_def=lib_branch_pvtrc, ...)
def corner_step1(context):
    library, branch = context.partition_key.keys_by_dimension["lib_branch"].split("__")
    pvtrc = context.partition_key.keys_by_dimension["pvtrc"]
    # ... per-partition body, same as before
```

## Scaling levels — what to do at each scale

> **Calibration anchor**: `demo/scale-lib/` (Brian, 2026-05-12,
> production-validated) shows that **1 lib × 460 branches × 21
> steps ≈ 10.6k partition records still fits SQLite**. The
> triggers below are revised down from an earlier draft that
> conflated "asset count" (placeholder limit) with "partition
> record count" (query throughput). See
> `memory/lessons_learned/_inbox/2026-05-12T140000-tl32rodan-lesson-12-level-calibration.md`
> for the back-story.

The level triggers below are **OR-conditions** — if any apply, advance.

### Level 1: small
**Trigger**: < ~1k partition records total, < 999 assets.

Default SQLite + any layout. Lesson 11's per-(lib, branch, step)
shape is fine for clarity.

### Level 2: medium (most production EDA flows land here)
**Trigger**: 1k–1M partition records, OR asset count > 999.

Two compounding levers:
- **Refactor to compact layout** → asset count drops to ≈ `K`
  (the step-type count). Trivially under SQLite limits.
- **Switch to Postgres** → no `?`-placeholder ceiling; bulk
  queries on 100k–1M partition rows are fine.

Brian's projected scale (1 lib × 64 branches × 21 steps ≈
26.8k partition rows per lib, × 6 libs ≈ 160k total) sits
**comfortably in Level 2** with compact + Postgres.

### Level 3: large
**Trigger** (any of):
- Postgres query latency on partition lookups becomes user-visible
  (typically > 1M partition rows AND queries hot-path on UI)
- Per-library failure isolation is required (one library's bad code
  shouldn't break the others' lineage view)
- Different teams own different libraries and want independent
  deploy cadence
- Need to scale beyond a single Python process's memory budget for
  asset graph (rare; ~50MB per 1k assets in 1.13.3)

**Per-library code location** decomposition: one gRPC
`dagster code-server` process per library. Webserver aggregates
lineage across locations (Day-7 cross-loc pattern).

```yaml
# workspace.yaml
load_from:
  - python_module: { module_name: pipelines, working_directory: ./svt,    location_name: svt }
  - python_module: { module_name: pipelines, working_directory: ./lvt,    location_name: lvt }
  # ... one per library
```

Per-library code-server cost: ~50 MB RAM idle + ~150 MB during
a run. 100 processes = ~15 GB idle. `multi_location/` demos
this at 3-library scale.

**Note**: pure cardinality (even "100 libraries") doesn't trigger
Level 3 if the partition-record count comes from branch×step×PVT
not library×branch — single-location compact + Postgres handles
it. Only advance to Level 3 when the **isolation / latency /
deploy-cadence** needs are real.

### Level 4: very large
**Trigger**: Need separate Dagster *instances* — not just code
locations — for strict tenant isolation (e.g. internal-vs-external
team), independent Dagster version pinning, or per-team Postgres
schema separation.

Each library/team gets its own Dagster deployment:
- Own Postgres DB / schema, own webserver, own `dagster.yaml`
- Coordination via shared filesystem or external orchestrator

Loses unified-graph; gains tenant isolation. Rarely needed.

### Level 5: re-evaluate the tier cut
**Trigger**: Tier-1 (Dagster) asset count itself > 1M.

This usually means leaf-level work that should be in **Tier-2**
(LSF / Slurm / batch scheduler — see lesson 13) is being modeled
as Dagster partitions instead. Pull the leaves out of Dagster's
partition store; let Tier-2 handle the fan-out opaquely.

Use Dagster only as a lineage / observability shell at the
higher-cardinality boundary. The `demo/scale-lib/` 4-layer
architecture is the canonical Tier-1 shape; the Tier-2 inside
is whatever your batch scheduler runs.

Don't reach for Level 5 until you've confirmed the cardinality
explosion is structural (not a layout problem) — Levels 2 + 3
solve most "Dagster is slow" complaints.

## Postgres migration — the actual steps

See [`POSTGRES_MIGRATION.md`](POSTGRES_MIGRATION.md) for the
step-by-step. Summary:

1. Install `dagster-postgres` wheel (in your venv or wheelhouse)
2. Edit `$DAGSTER_HOME/dagster.yaml` — add `storage.postgres.postgres_db` stanza
3. `dagster instance migrate` (creates the schema)
4. Restart webserver + daemon
5. Verify with `dagster instance info` showing `postgres` storage

Your existing SQLite data does NOT auto-migrate to Postgres.
Existing run history stays in SQLite or is lost. Plan a cutover
window.

## Common gotchas

- **MultiPartitions 2D limit in 1.13.3** — already in
  `database/.../partitions.md`. Affects compact layout: we
  collapse `(library, branch)` into one composite key
  (`lib_branch`).
- **Backfill UI with 5,000+ partition cells** — the heatmap
  renders but is unwieldy. Filter by `lib_branch=X` or use
  CLI `--partition 'lib_branch=svt__corner|pvtrc=tt_25'`.
- **Cross-location asset deps** — must use the Day-7 fix:
  `deps=[AssetKey(["other_loc", "asset_name"])]`, NOT
  `AssetSpec(...)` in `Definitions(assets=...)`. See
  `database/.../cross-location.md`.

## Cardinality calculator usage

```bash
# Brian's production scale (default)
python -m cardinality_calc

# Your projected scale (libraries, branches, step_types, pvtrcs)
python -m cardinality_calc 100 50 15 20
```

Output includes:
- Asset count under both layouts
- Partition record count
- SQLite headroom (old & new versions)
- Recommendation (refactor / Postgres / code-locations / scrap)

## Related cheatsheet entries

- `database/dagster-1.13.3/docs/partitions.md` — MultiPartitions API
- `database/dagster-1.13.3/docs/cross-location.md` — Day-7 fix
- `database/dagster-1.13.3/docs/style-a-vs-b.md` — Style B fan-in
- `database/dagster-1.13.3/docs/_inbox/2026-05-10T2340-ui-scaling-techniques.md`
  — earlier observation that informs this lesson
- Lesson 11 — the cardinality baseline this refactors away from
