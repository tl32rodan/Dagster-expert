# Partitions — static, dynamic, multi (1.13.3 limits)

**Tested against Dagster 1.13.3.**

## Static partitions

Hard-coded set of keys, known at code-load time.

```python
from dagster import StaticPartitionsDefinition, asset, AssetExecutionContext, MaterializeResult

CORNERS = ["ff_125c", "tt_25c", "ss_m40c", "ss_125c"]
corner_partitions = StaticPartitionsDefinition(CORNERS)

@asset(partitions_def=corner_partitions)
def corner_summary(context: AssetExecutionContext) -> MaterializeResult:
    key = context.partition_key   # one of CORNERS, set per run
    ...
```

UI shows a partition heatmap on the asset. CLI:
```bash
dagster asset materialize -m mod --select corner_summary --partition ff_125c
```

## Dynamic partitions

Set of keys grows over time, populated by sensor or
`add_dynamic_partitions()`.

```python
from dagster import DynamicPartitionsDefinition

wafer_partitions = DynamicPartitionsDefinition(name="wafer_id")

@asset(partitions_def=wafer_partitions)
def wafer_report(context): ...
```

Add keys at runtime:
```python
context.instance.add_dynamic_partitions(
    partitions_def_name="wafer_id",
    partition_keys=["W2401-A", "W2401-B"],
)
```

Use when keys aren't known in advance (new wafer scans, dynamic
test list).

## MultiPartitionsDefinition — **2 dimensions max in 1.13.3**

```python
from dagster import MultiPartitionsDefinition

partitions = MultiPartitionsDefinition({
    "corner": StaticPartitionsDefinition(CORNERS),
    "instance": StaticPartitionsDefinition(["INV", "BUF", "MUX"]),
})

@asset(partitions_def=partitions)
def per_corner_per_instance(context):
    keys = context.partition_key.keys_by_dimension
    corner = keys["corner"]
    instance = keys["instance"]
    ...
```

**1.13.3 hard limit**: exactly 2 dimensions. Adding a 3rd raises:

```
DagsterInvalidInvocationError: Dagster currently only supports
multi-partitions definitions with 2 partitions definitions.
```

## Modeling N>2 dimensions in 1.13.3

Three escape hatches:

### 1. Composite key (collapse 2 dims into 1)

```python
EM_HT_PAIRS = ["em_lo__ht_low", "em_lo__ht_mid", ...]   # cross-product

partitions = MultiPartitionsDefinition({
    "corner":  StaticPartitionsDefinition(CORNERS),
    "em_ht":   StaticPartitionsDefinition(EM_HT_PAIRS),
})
```

UI shows a 2D heatmap; em_ht keys are concatenated strings.
Slightly ugly but functional.

### 2. Drop to concrete assets (Route A — see complex deps lesson)

Generate one `@asset` per valid (corner, em, ht) tuple at
definition time. Lots of nodes in the lineage UI but fully
expresses sparse-matrix dependencies.

### 3. Dynamic partitions registered at runtime

If valid combos become known at runtime (e.g. from an external
catalog), use `DynamicPartitionsDefinition` and register only
the valid combos as partition keys.

## `--select` syntax with partitions

```bash
# Single partition
dagster asset materialize -m mod --select corner_summary --partition ff_125c

# Multiple specific partitions
dagster asset materialize -m mod --select corner_summary --partition ff_125c,tt_25c

# All partitions (backfill)
dagster job backfill -m mod -j __ASSET_JOB --partition-set corner_summary_partition_set
```

## Selection by tag/group across partitioned assets

```bash
dagster asset materialize -m mod --select 'tag:foo=bar' --partition ff_125c
dagster asset materialize -m mod --select 'group:my_group' --partition ff_125c
```

## Common gotchas

- **"Cannot materialize asset without specifying a partition"** —
  partitioned asset, run launched without picking one. UI defaults
  to latest; CLI requires `--partition`.
- **MultiPartitions arity** — 2D limit, see above.
- **Dynamic partition keys not visible after registering** — UI
  may need a code location reload to see new keys; `dagster
  asset list` shows them after the next refresh.
- **Cross-asset mapping degrades to "all partitions"** — the two
  assets hold *different* `partitions_def` objects. Define one
  module-level singleton and import it everywhere. See
  [`STANDARD_USAGE.md`](STANDARD_USAGE.md) §9a.
- **Many-to-many cross-asset deps** — do NOT subclass
  `PartitionMapping`; pre-compute a memoized built-in
  `StaticPartitionMapping`. See [`STANDARD_USAGE.md`](STANDARD_USAGE.md) §3.2.

## Related

- The prescribed path for partition mapping & execution:
  [`STANDARD_USAGE.md`](STANDARD_USAGE.md) §3, §9
- Examples: [`04_partitioned.py`](../examples/04_partitioned.py),
  [`05_multipartition_2d.py`](../examples/05_multipartition_2d.py)
