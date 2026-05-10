# lab11 · multi-library + UI scaling

**Time**: 60–90 min · **Prerequisites**: lessons 09 (real flow), 10
(branched flow)

## Why this lab exists

Real TSMC AP characterization runs **multiple libraries** in
parallel — typically Vt classes (svt / lvt / ulvt / hvt / elvt /
ulvthp). Each library is the same pipeline shape (lesson 10's
branched flow), but operates on different transistor data with
different sign-off owners.

This raises two structural questions:

1. **How to express it?** Programmatic asset generation per
   library, with `key_prefix` for namespacing.
2. **How to keep the lineage UI navigable?** When 6 libs × 3
   branches × 3 steps = 54 assets, the default lineage view is a
   wall of nodes. Solution: `group_name` for visual collapse.

## Brian's question — should we collapse `branch` into a partition axis?

### TL;DR: don't.

Real concern: 30+ branches → 90+ asset chains → unworkable lineage.

**Tempting fix**: instead of separate assets per branch, fold
branch into a partition axis (like `(branch, pvtrc)`). One asset
"step1" with N×P partitions instead of N assets.

**Why it doesn't help**:

| Question | Branch-per-asset (lessons 10/11) | Branch-as-partition (rejected) |
|---|---|---|
| UI: cross-branch dep visibility | **Visible** as edges in lineage graph | **Invisible** — Dagster sees only "step1 → step2", body internals hidden |
| Cross-branch dep mechanism | `deps=[AssetKey(...)]` + Style B body | Style B body only (no `deps=` on self-cross-partition) |
| Per-partition pipelining | ✓ (Identity mapping) | ✓ (Identity mapping) |
| Stale propagation across branches | ✓ Dagster tracks via deps | ✗ Dagster has no signal — invisible deps mean invisible staleness |
| Backfill failure isolation | ✓ Per-branch failures isolated | ✓ Per-partition isolated |
| Asset count (3 branches × 3 steps) | 9 assets | 1 asset, 27 partitions |

**The real lever for UI scaling is grouping, not collapsing**:
- `key_prefix=[<library>]` — namespaces asset keys, lineage UI
  shows them as nested folders
- `group_name=f"{library}_{branch}"` — colors assets by group,
  UI lets you collapse by group_name
- The "Groups" view in the asset graph collapses each group_name
  into a single visual node; you click to expand

This lesson uses both. With 24 assets across 9 group_names, the
lineage UI is navigable. **Same techniques scale to 540 assets
across 36 groups** — TSMC production scale, manageable.

## Per-partition pipelining — clarification

Concern: "if branch is a partition, must all step-N partitions
finish before any step-(N+1) partition starts?"

**No.** Dagster's default `IdentityPartitionMapping` makes each
partition independent: `step2(tt_25)` only waits for `step1(tt_25)`,
not for `step1(ff_125)` or `step1(ss_m40)`. The whole point of
partitioning is per-partition pipelining.

This holds whether branch is its own asset or folded into a
partition axis. The visualization choice doesn't affect runtime
semantics here.

## DAG shape

```
                    cell_list (shared, group="shared")
                          │
        ┌─────────────────┼──────────────────┐
        │                 │                  │
        ▼                 ▼                  ...
        svt subgraph     lvt subgraph
   ┌──────────────┐  ┌──────────────┐
   │ corner_stepN │  │ corner_stepN │       (full PVTRC: ff/tt/ss)
   │      ↓       │  │      ↓       │
   │ lvf_stepN    │  │ lvf_stepN    │       (subset: tt only)
   │      ↓       │  │      ↓       │
   │ em_stepN     │  │ em_stepN     │       (subset: ff/ss)
   │      ↓       │  │      ↓       │
   │ lib_signoff  │  │ lib_signoff  │
   └──────┬───────┘  └──────┬───────┘
          │                 │
          └────────┬────────┘
                   ▼
          cross_library_signoff (group="shared")
```

Each library has identical shape; the asset keys are namespaced:
`["svt", "corner_step1"]`, `["lvt", "corner_step1"]`, etc.

## Cardinality

| Asset family | Per library | × 2 libs | Notes |
|---|---|---|---|
| corner_step{1,2,3} | 3 partitions × 3 steps = 9 | 18 | full PVTRC |
| lvf_step{1,2,3}    | 1 × 3 = 3 | 6 | tt only |
| em_step{1,2,3}     | 2 × 3 = 6 | 12 | extremes |
| lib_signoff        | 1 | 2 | per-library final |
| **per-library total**  | 11 assets, 18 partition runs | | |
| Plus shared: cell_list (1) + cross_library_signoff (1) | | | |
| **TOTAL**          | | **24 assets, 36 partition runs** | |

Scales linearly with library count. 6 libs → 64 assets, 108
partition runs (still <1 min smoke time).

## Run it

### Smoke

```bash
source ~/dagster-venv/bin/activate
cd ~/projects/personal-assistant/personalities/dagster-tutor/learn/11-multi-library
python -m _smoke
```

~10s end-to-end. Verifies 38 artifact files match expected.

### Interactive

```bash
dagster dev -m pipelines
# open http://127.0.0.1:3000
```

In the lineage UI, observe:

1. **Default view** — 24 nodes. Visually busy but navigable.
2. **Group view** (top-left toggle) — each `group_name` collapses
   into one node. You see ~9 nodes (svt_corner / svt_lvf /
   svt_em / svt_signoff / lvt_corner / lvt_lvf / lvt_em /
   lvt_signoff / shared). Click to expand each.
3. **Filter by group** — sidebar lets you isolate one library
   end-to-end.
4. **Asset key** view — assets are listed with prefixed paths
   (`svt/corner_step1`, etc.); namespacing is obvious.

## How to scale to 6 libraries

In `pipelines/partitions.py`:

```python
LIBRARIES = ["svt", "lvt", "ulvt", "hvt", "elvt", "ulvthp"]
```

Reload. The `_build_library_assets()` factory generates the
subgraph for each new library. Asset count: 6 × 11 + 2 = 68
assets; partition runs: 6 × 18 + 2 = 110.

UI still navigable: 6 libraries × 4 group_names = 24 group_name
collapsable nodes. Lineage UI handles this fine.

## How to scale to 30 branches (Brian's real concern)

If a future flow has 30 branches per library (not 3), the same
factory pattern works:

```python
BRANCHES = ["corner", "lvf", "em", "noise", "stress", "drv", ...]   # 30
```

Each branch needs its own `partitions_def` and dep wiring; the
factory handles the rest. Asset count: 6 libs × 30 branches × 3
steps = 540 assets. Group_names: 6 × 30 = 180 groups. The Group
view in lineage UI is still the way to navigate — you only ever
see ~180 collapsed nodes at a time, expand the one you need.

If 540 assets becomes a Dagster instance load problem (it
shouldn't until 1000s), the next escape hatch is **per-library
code locations** (`workspace.yaml` lists each library as a
separate gRPC code-server). See lesson 07 for cross-location
mechanics.

## Common gotchas

- **`group_name` per asset, not per asset family.** Each `@asset`
  decorator carries its own `group_name=`. The factory function
  in `asset.py` sets it consistently per branch.
- **`key_prefix` prefix order matters.** The asset key path is
  `[*key_prefix, name]`. `key_prefix=[lib]` with `name="corner_step1"`
  → `AssetKey(["svt", "corner_step1"])`. Cross-references must
  spell out the full key path.
- **Default IdentityPartitionMapping needs identical
  partition_def OBJECTS (not just same content)**. We pass the
  same module-level `corner_partitions` to every corner asset
  across all libraries — they share the def object, so identity
  mapping works.
- **Smoke ordering matters.** The driver materializes per-library
  in dep order: corner before lvf/em (cross-branch dep).
  Materialize one library completely before moving to the next
  to avoid leakage.

## Now-try

1. **Add a 3rd library** (`ulvt`). Edit `LIBRARIES` in
   `partitions.py`. Reload. Lineage shows the new subgraph
   appears with same shape. Materialize it independently.

2. **Toggle UI from default to Group view** in the lineage UI
   (top-left controls, "Groups" tab or filter). Compare
   navigability. With 6+ libraries, default is unworkable; group
   view stays clean.

3. **Trigger cross-library dep failure**: comment out the `deps=`
   on `cross_library_signoff` for one library. Reload.
   Materialize cross_library_signoff. The asset will still
   "succeed" (no Dagster-level dep), but the cross signoff's
   content for that library will be stale or missing — silent
   failure, demonstrating why explicit deps matter.

## Related cheatsheet entries

- `dagster-librarian/database/dagster-1.13.3/docs/asset-basics.md`
  — `key_prefix`, `group_name`, asset key conventions
- `dagster-librarian/database/dagster-1.13.3/docs/style-a-vs-b.md`
  — programmatic asset generation pattern
- Lesson 10's README — single-library branched flow (this lesson
  is one library on top of that)
