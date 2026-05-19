# lab17 · cross-partition incremental rerun

**Time**: 60 min · **Prerequisites**: lessons 02 (deps), 03
(partitions), 06d (data_version basics)

> 💡 This is the lesson that closes the gap between "I know
> partitions exist" and "I trust Dagster to only re-run the
> partitions that actually need it after I edit upstream."

## The promise

In an AP characterization flow with 21 steps × 46 branches, you
edit step-5's input for branch `corner_ff`. You want Dagster to
mark **just** step-5..21 of `corner_ff` as stale — not every
partition of every downstream step. Re-materializing 1 partition
should be 1 partition of work, not 46.

That is the **cross-partition incremental** contract. It works
when:

1. Each asset's `data_version` is computed from output content
   (not constants).
2. Cross-asset partition mapping is correctly declared (Identity
   for same partition def, Static otherwise).
3. The chain is unbroken — every intermediate asset propagates
   data_version correctly.

When any of these breaks, staleness silently disappears, and
"incremental rerun" quietly becomes "manual full backfill". This
lesson walks you through each piece.

## The three sub-labs

| Sub-lab | What it teaches | Time |
|---|---|---|
| [`17a-identity`](17a-identity/) | Default IdentityPartitionMapping — same partition def on both sides. The "happy path". | ~20m |
| [`17b-static-mapping`](17b-static-mapping/) | `StaticPartitionMapping` — different partition defs, fan-out + sparse routing. The pattern used by `demo/scale-lib/`. | ~25m |
| [`17c-data-version-trap`](17c-data-version-trap/) | The constant-hash trap — staleness silently breaks when an intermediate hash ignores upstream content. The #1 production bug for this feature. | ~15m |

Run each in order. Each sub-lab's README is standalone — read
it, run it, observe the partition heatmap in the UI.

## How Dagster decides "stale" for a partitioned asset

For partition `P` of downstream asset `D` consuming partition `Q`
of upstream `U`:

1. The latest materialization of `U` at partition `Q` carries a
   `data_version` (the string you set via
   `MaterializeResult(data_version=DataVersion(...))`).
2. `D`'s last materialization at `P` recorded what it consumed —
   i.e. the upstream's `data_version` at the time `D[P]` ran.
3. If the *current* upstream `data_version` != *what `D[P]` last
   consumed* → `D[P]` is stale.

This check is per-partition and per-upstream-edge. With 4 corners
and 1 upstream changing, only the corner with the mismatch goes
stale. With a fan-out mapping (1 upstream → 3 downstream
partitions), all 3 downstream partitions see the mismatch and go
stale.

## Common shape across the sub-labs

Each sub-lab follows the same drill:

1. Materialize everything → all partitions fresh.
2. Edit ONE upstream partition's payload.
3. Reload code locations; re-materialize that one partition.
4. Observe UI: which downstream partitions are now stale?
5. Re-materialize only the stale ones.

The observation in step 4 is the lesson. Each sub-lab makes that
step show something slightly different (identity mapping = 1:1,
static mapping = N:M, broken chain = nothing at all).

## What this lesson is NOT covering

- **Auto-materialize** — see lesson 19. This lesson is manual
  rerun; you click "Materialize" yourself.
- **Cross-code-location staleness** — see lesson 18. This lesson
  is single-location.
- **Time-based partitions / sensors triggering rebuilds** —
  lessons 14/15.

## Pitfalls

- **`Data version` column in UI matches between two materializations
  → your hash is constant.** Cheatsheet:
  [`data-version-and-staleness.md`](../../database/dagster-1.13.3/docs/data-version-and-staleness.md).
  17c walks through it.
- **`mapping target partitions not in the downstream partitions
  definition`** — your `StaticPartitionMapping` targets keys the
  downstream doesn't declare. PR #7 hit this in
  `demo/scale-lib/`; the fix is to filter mapping values by the
  downstream's actual partition keys.
- **Re-loading code locations doesn't trigger staleness** — only
  re-materializing upstream with a different `data_version` does.
  Reload reads the new source; Step 3 (re-materialize one
  partition) writes the new `data_version` the downstream is
  compared against.
- **Same partition stale across BOTH `mid_corner` and
  `final_corner`** even though you only re-ran `raw_corner` —
  expected. Dagster propagates staleness through the entire
  downstream graph until you re-run the intermediates. Cheap
  shows of strength to anyone who claims Dagster "needs you to
  re-run everything".
