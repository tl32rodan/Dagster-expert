# lab3 · partitions — the Dagster "for-loop"

**Time**: 45 min · **Prerequisites**: lessons 01, 02

## What you'll learn

- Why `@asset` + a partitions definition replaces a for-loop
- Static vs dynamic partitions
- Materializing one partition, a range, or all of them
- The `dagster/partition` tag in run history

## Why partitions exist

In an EDA flow you don't run "the corner analysis" once — you run
it for each (corner × instance × Vt). The naive Python solution
is a for-loop: `for c in corners: run_thing(c)`. Dagster turns
that into one **partitioned asset** where each partition key is
one corner.

Benefits:
- Materialize one corner without re-running others.
- The UI shows a heatmap of which partitions are fresh / stale /
  failed.
- Partitions can be backfilled independently.

## The lesson in 60 seconds

```bash
cd 03-partitions
dagster dev -m by_corner
# open http://127.0.0.1:3000
```

In the Assets graph: `corner_summary` shows up with a partitions
strip. Click it; you'll see four partitions: `ff_125c`, `tt_25c`,
`ss_m40c`, `ss_125c`.

### Materialize ONE partition

Click `corner_summary`, click "Materialize", select partition
`ff_125c`, run. Only that partition runs.

```bash
dagster asset materialize -w workspace.yaml --select corner_summary --partition ff_125c
```

### Materialize a range (backfill)

Use the UI's backfill panel, or:

```bash
dagster job backfill -w workspace.yaml -j __ASSET_JOB \
    --partition-set corner_summary_partition_set
```

### Re-materialize only the stale ones

The UI's "Materialize" → "Materialize stale" button picks only
partitions whose data_version is older than upstream.

## Static vs dynamic partitions

**Static** (`StaticPartitionsDefinition`): the set of partition
keys is fixed and known at code-load time. Use for corners, IP
blocks, foundries, anything you'd hard-code.

**Dynamic** (`DynamicPartitionsDefinition`): the set grows over
time, populated by sensors or `add_dynamic_partitions`. Use for
"new wafer ID arrived in the data lake" — the keys aren't known
in advance.

This lab uses static. Dynamic is a 1-line change but a different
operational discipline.

## Now try

### Try 1 · Materialize all four corners, then edit one

Edit `by_corner/asset.py` so that the `ss_m40c` branch returns
different bytes. Reload the code location. The lineage view shows
`ss_m40c` as stale (yellow); the others stay green.

### Try 2 · Add a fifth corner

Add `"sf_85c"` to the `CORNERS` list. Reload. The new partition
appears as a missing (gray) cell. Materialize just it.

### Try 3 · Multiple partitions in one run

CLI:
```bash
dagster asset materialize -w workspace.yaml --select corner_summary \
    --partition ff_125c,tt_25c
```

The run executes the asset twice in sequence (once per partition).
Both materializations land in the same run; you'll see two
materialization events in its event log.

## Common pitfalls

- **No partitions strip in the UI**: you forgot
  `partitions_def=...` on the `@asset` decorator.
- **"Cannot materialize asset without specifying a partition"**:
  partitioned asset, run launched without picking one. UI defaults
  to the latest partition; CLI requires `--partition`.
- **Partition keys don't show up after editing**: code location
  cache. Click "Reload" or restart `dagster dev`.

## Cheat sheet

```python
from dagster import (
    asset,
    StaticPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
)

CORNERS = ["ff_125c", "tt_25c", "ss_m40c", "ss_125c"]
corner_partitions = StaticPartitionsDefinition(CORNERS)

@asset(partitions_def=corner_partitions)
def corner_summary(context: AssetExecutionContext) -> MaterializeResult:
    key = context.partition_key      # "ff_125c", etc.
    ...
```

CLI:
```bash
# Single partition
dagster asset materialize --select <name> --partition <key>

# Multi-partition in one run
dagster asset materialize --select <name> --partition k1,k2,k3

# Backfill
dagster job backfill -j __ASSET_JOB --partition-set <name>_partition_set
```
