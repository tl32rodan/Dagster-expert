# lab8 · complex dependency patterns

**Time**: 90 min · **Prerequisites**: lessons 01, 02, 03, 07

## What you'll learn

- How to model **sparse-matrix dependencies** (the AP
  characterization shape: corner × instance × Vt with not-all
  combinations valid)
- Two routes to the same DAG, with different ergonomics:
  - **Route A** — concrete assets, one per (corner, instance,
    Vt) tuple
  - **Route B** — `MultiPartitionsDefinition` + `PartitionMapping`
- When to pick which

## The shape we're modeling

Imagine the AP characterization DAG:

```
step0 (corner-only)              4 partitions: corner ∈ {ff, tt, ss, sf}
  ↓
step1..5 (corner × em × ht)      20 partitions: corner × em × ht (some combos invalid)
  ↓
step6 (multi-deps fan-in)        depends on step5 + step0 of same corner
```

The matrix is **sparse**: not every (corner, em, ht) is valid; the
flow only runs the valid combos. Dependencies cross partition
axes: step6 needs ALL step5 partitions of one corner, plus the
matching step0.

## Route A — concrete assets

Generate one `@asset` per valid combo at definition time. The DAG
becomes a flat list of explicit assets connected by explicit
`deps=`.

```python
VALID_COMBOS = [
    ("ff", "em_lo", "ht_low"),
    ("ff", "em_hi", "ht_high"),
    ("tt", "em_mid", "ht_mid"),
    # ... only the combos that actually exist
]

assets = []
for corner, em, ht in VALID_COMBOS:
    @asset(
        name=f"step5__{corner}__{em}__{ht}",
        deps=[AssetKey(f"step0__{corner}")],
    )
    def step5(...): ...

    assets.append(step5)
```

**Pros**: every dep is visible in the lineage graph. Sparse
matrix is honored exactly. Easy to debug — you can see in the
UI exactly which combos exist.

**Cons**: lots of asset nodes (clutters UI when N is large).
Code is generated — readers need to understand the pattern. Not
backfill-friendly: each asset is its own thing.

### When to choose A
- Small-to-medium combo count (< ~50 valid combos)
- Combos are heterogeneous in code (each has slightly different
  body)
- You want every node visible in the lineage UI

## Route B — MultiPartitionsDefinition + PartitionMapping

Use a single `@asset` with a multi-dim partition space; encode
"valid combos only" via a guard inside the asset body or via
a `PartitionMapping` that skips invalid ones.

**⚠ Dagster 1.13.3 limit**: `MultiPartitionsDefinition` supports
**only 2 dimensions**. If your real matrix has 3+ dims (corner ×
em × ht), you must collapse two of them into a composite key
(e.g. `"em_lo__ht_low"`) — that's what the example code in this
lesson does. With deeper hierarchies, this gets ugly fast and
becomes a real argument for Route A. (For unbounded combos,
`DynamicPartitionsDefinition` registered at runtime is another
escape hatch — out of scope for this lab.)

```python
from dagster import (
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    PartitionMapping,
)

# 2-dim form (1.13.3 max). em+ht collapsed into a composite key.
step5_partitions = MultiPartitionsDefinition({
    "corner": StaticPartitionsDefinition(["ff", "tt", "ss", "sf"]),
    "em_ht":  StaticPartitionsDefinition([
        "em_lo__ht_low", "em_lo__ht_mid", "em_lo__ht_high",
        "em_mid__ht_low", "em_mid__ht_mid", "em_mid__ht_high",
        "em_hi__ht_low", "em_hi__ht_mid", "em_hi__ht_high",
    ]),
})

@asset(partitions_def=step5_partitions)
def step5(context): ...
```

To express "step6 of corner X depends on ALL step5 partitions of
corner X plus step0 of corner X", you write a custom
`PartitionMapping` that fans out across the `em_ht` dimension for
the matching `corner`.

**Pros**: one asset node in the UI, one decorator, scales to
thousands of combos without code generation. Backfill UI handles
multi-axis selection naturally.

**Cons**: invalid combos materialize as failed/missing partitions
unless you guard inside the asset body. The `PartitionMapping` is
non-trivial to write correctly. Lineage view shows one node, so
debugging which combo failed requires drilling into partitions.

### When to choose B
- Large combo count (hundreds to thousands)
- Combos are homogeneous in code (same body, different inputs)
- You'll backfill in batches (e.g. "all `corner=ff` partitions")

## Side-by-side comparison

| Question | Route A (concrete) | Route B (MultiPartitions) |
|---|---|---|
| Add a new valid combo | Add to `VALID_COMBOS`, code re-generates | Just materialize; partition exists in space |
| Remove an invalid combo | Remove from `VALID_COMBOS` | Guard inside asset body or via `PartitionMapping` |
| UI clarity for N=10 combos | Excellent | Mediocre |
| UI clarity for N=10,000 combos | Unworkable | Excellent |
| Code complexity | Loop generating decorated functions | Custom `PartitionMapping` |
| Backfill ergonomics | One run per asset | One run, multi-axis filter |
| Type-checking | Each asset is a real function | Body uses `context.partition_key` strings |

## The lesson in 60 seconds

```bash
cd 08-complex-deps
dagster dev -w workspace.yaml
# open http://127.0.0.1:3000
```

You'll see two code locations side by side:
- `route_a` — concrete-assets version
- `route_b` — MultiPartitions version

Click each to compare. They produce equivalent results; the
modeling difference is in the asset graph.

## Now try

### Try 1 · Pick a combo and trace its lineage in route_a

In the `route_a` graph, click `step6__ff`. The upstream chain
shows `step0__ff`, `step5__ff__em_lo__ht_low`, `step5__ff__em_hi__ht_high`,
... — all explicit nodes.

### Try 2 · Same combo in route_b

In `route_b` graph, click `step6`. Upstream is just `step5` and
`step0` (with partition mappings noted). Drill into `step5`'s
partition heatmap to see the per-combo state.

### Try 3 · Add a new combo

In `route_a/assets.py`, add `("sf", "em_mid", "ht_mid")` to
`VALID_COMBOS`. Reload. New asset node appears.

In `route_b`, no code change — `MultiPartitionsDefinition` already
includes that combo. Materialize it directly.

### Try 4 · Express "skip combo (sf, em_lo, ht_low)"

In `route_a`: just don't add it to `VALID_COMBOS`. Done.

In `route_b`: guard inside the asset body:

```python
@asset(partitions_def=step5_partitions)
def step5(context):
    keys = context.partition_key.keys_by_dimension
    if (keys["corner"], keys["em"], keys["ht"]) == ("sf", "em_lo", "ht_low"):
        raise SkipNotebook()  # or just return; or context.log.warning + early
```

Or use a `PartitionMapping` that excludes that combo. (Harder.)

## Decision rule of thumb

> **Start with Route A.** Move to Route B only when the asset
> count is unwieldy in the UI, OR you need backfill ergonomics
> for thousands of combos.

For the LENS PoC scale (TSMC AP characterization, dozens to
hundreds of valid combos), Route A is usually clearer.

## Common pitfalls

- **Route A: stale codegen**: edited the loop body but old asset
  nodes still appear. Restart `dagster dev`; generator-style asset
  registration is cached.
- **Route B: partition mapping mis-fan-out**: easy to write a
  mapping that fans out the wrong dimension. Test by clicking
  through the lineage UI to verify the partition resolution.
- **Both routes: cross-location**: this lab keeps each route in
  its own location for clarity. In production the routes typically
  live together, but split if (a) different teams own them or (b)
  one is mature and the other is experimental.

## Cheat sheet

```python
# Route A — concrete assets
assets = []
for corner, em, ht in VALID_COMBOS:
    @asset(
        name=f"step5__{corner}__{em}__{ht}",
        deps=[AssetKey(f"step0__{corner}")],
    )
    def step5(): ...
    assets.append(step5)
defs = Definitions(assets=assets)
```

```python
# Route B — MultiPartitions
from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition

space = MultiPartitionsDefinition({
    "corner": StaticPartitionsDefinition([...]),
    "em":     StaticPartitionsDefinition([...]),
    "ht":     StaticPartitionsDefinition([...]),
})

@asset(partitions_def=space)
def step5(context):
    k = context.partition_key.keys_by_dimension
    # k["corner"], k["em"], k["ht"]
```

## See also

- Lesson 03 (partitions basics) — `MultiPartitionsDefinition` is
  a generalization of `StaticPartitionsDefinition`
- Lesson 07 (cross-location) — the routes here live in separate
  locations
