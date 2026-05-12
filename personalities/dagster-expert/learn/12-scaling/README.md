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

### Level 1: small (< 100 assets even uncollapsed)
Default SQLite + any layout. Lesson 11's per-(lib, branch, step)
shape is fine for clarity.

### Level 2: medium (100–4,500 assets — Brian's current scale)
Two options, pick either:
- **Stay on SQLite, refactor to compact** → 15 assets total, fits 999-limit
- **Stay on lesson-11 shape, switch to Postgres** → no asset-count limit, easier migration

**Best**: both. Compact + Postgres = belt-and-suspenders + lowest UI latency.

### Level 3: large (5,000–75,000 assets — 100 libraries)
Compact alone keeps asset count low (15), BUT partition record
count balloons (1.5M). Single-location queries on 1.5M rows
get slow.

**Per-library code location** is the right decomposition: one
gRPC `dagster code-server` process per library. Each loads only
~15 assets × 1 library's branches/PVTRCs. The webserver
aggregates lineage across all code locations (Day-7 cross-loc
pattern).

```yaml
# workspace.yaml
load_from:
  - python_module: { module_name: pipelines, working_directory: ./svt,    location_name: svt }
  - python_module: { module_name: pipelines, working_directory: ./lvt,    location_name: lvt }
  - python_module: { module_name: pipelines, working_directory: ./ulvt,   location_name: ulvt }
  # ... 97 more
```

Per-library code-server process cost on a 32-core / 64GB host:
~50 MB RAM idle + ~150 MB during a run. 100 processes = ~15 GB
idle, well within budget.

`multi_location/` demos this with 3 libraries — same shape
generated programmatically. Add libraries by copying the
template and appending to `workspace.yaml`.

### Level 4: very large (75,000+ assets — 100+ libs × full sweep)
Multiple Dagster *instances*, not just code locations. Each
library team gets its own Dagster deployment:
- Own Postgres DB / schema
- Own webserver
- Own `dagster.yaml`

Coordination via shared filesystem or external orchestrator. Loses
the unified-graph property — but you gain full tenant isolation.

Rarely needed in practice unless library teams have strict
separation requirements.

### Level 5: scrap Dagster as orchestrator (Brian's option c)
At 1M+ asset declarations or strict latency budgets, Dagster's
event-log overhead becomes the bottleneck. Use Dagster only as
a lineage/observability shell; execute via custom agent skill
that:
- Builds the dep DAG from a YAML / config file
- Calls subprocess for each step in dep order
- Reports completions back to Dagster via Pipes or GraphQL

Loses retry/queue/scheduling — agent must reimplement.

Don't go here until Level 3 has demonstrably failed. The
maintenance cost is significant.

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
