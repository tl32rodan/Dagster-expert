# 17a · identity partition mapping — default cross-partition wiring

**Time**: ~20 min · **The "happy path" baseline.**

## Setup

```bash
cd 17a-identity
dagster dev -m identity
# UI: http://127.0.0.1:3000
```

Two assets, both partitioned by the same 4 corners
(`ff_125c`, `tt_25c`, `ss_m40c`, `ss_125c`). No explicit
`PartitionMapping` is given → Dagster uses
`IdentityPartitionMapping` by default: downstream partition `X`
depends on upstream partition `X`.

## Walkthrough

### Step 1 · materialize everything

UI → backfill ALL partitions on `raw_corner`, then on
`mid_corner`. Or via CLI:

```bash
dagster asset materialize -m identity --select raw_corner --partition ff_125c,tt_25c,ss_m40c,ss_125c
dagster asset materialize -m identity --select mid_corner --partition ff_125c,tt_25c,ss_m40c,ss_125c
```

UI: both assets show all 4 partitions green / fresh.

### Step 2 · change ONE upstream partition's content

Edit `identity/asset.py`. Find:

```python
payload = f"raw_corner__{key}__rev=1".encode()
```

Change `rev=1` to `rev=2`. (Bumping any byte of the payload is
enough — the point is upstream's content + `data_version` must
actually change.)

Reload code locations in the UI (or restart `dagster dev`).

### Step 3 · re-materialize ONE upstream partition

UI: click `raw_corner` → Materialize → pick `ff_125c` only →
Launch.

```bash
dagster asset materialize -m identity --select raw_corner --partition ff_125c
```

### Step 4 · observe selective staleness

UI: open `mid_corner`. The partition heatmap should now show:

- `ff_125c` — **stale** (yellow `↻`) — its upstream changed
- `tt_25c`, `ss_m40c`, `ss_125c` — still **fresh** (green)

That is the cross-partition incremental promise: changing one
upstream partition only flags the *one* downstream partition
that consumes it. Backfilling all 4 would be wasted work.

### Step 5 · re-materialize ONLY the stale partition

UI → click `mid_corner` → Materialize → it pre-fills to
`ff_125c` (the stale one). Launch. ~1s, done.

```bash
dagster asset materialize -m identity --select mid_corner --partition ff_125c
```

All 4 downstream partitions are fresh again. The other 3 never
ran.

## Verify what happened

UI → `mid_corner` → **Materializations** tab. For each partition:

- `ff_125c` row: two materializations (before + after). The
  second has a different `Data version`.
- The other 3 partitions: one materialization each, unchanged.

CLI sanity check:

```bash
ls -la /tmp/dagster-17a-out/
# raw_ff_125c.bin mtime is newer than the other 3
# mid_ff_125c.bin mtime is also newer
# the other 6 files (tt_25c, ss_m40c, ss_125c × raw/mid) are untouched
```

## Why this works

1. `mid_corner` is partitioned with the same `corner_partitions`
   as `raw_corner`. Dagster infers the dep using
   `IdentityPartitionMapping`: partition `X` of mid depends on
   partition `X` of raw, no cross-partition fan-out.
2. `mid_corner`'s `data_version` is `hash(b"mid_of:" + upstream_bytes)`,
   so it actually changes when upstream's file content changes.
   (If we hashed a constant, the chain would break — that's the
   trap in **17c**.)
3. Dagster compares the `data_version` of the upstream
   materialization that mid_corner *last consumed* against the
   *current* upstream version per partition. Mismatch on `ff_125c`
   only → only `ff_125c` is stale.

## Pitfalls

- **No partition goes yellow after editing raw_corner + reload** —
  Step 3 (re-materialize one upstream partition) is what writes the
  new `data_version`. Reload reads the new source; the materialize
  step updates the instance store value the downstream is compared
  against.
- **`mid_corner` shows ALL partitions stale after you only
  materialized one of raw** — make sure `mid_corner` uses
  `deps=[AssetKey("raw_corner")]`, not `deps=[AssetKey(["raw_corner", "..."])]`
  with extra path. Path mismatch = no inferred mapping.

## What to try next

→ **17b** — what if upstream and downstream have DIFFERENT
partition sets and you need to wire them with
`StaticPartitionMapping`? That's the production case
(`demo/scale-lib/` does this for branch → branch).
