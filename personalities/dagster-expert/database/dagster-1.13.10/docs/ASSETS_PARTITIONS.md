# ASSETS_PARTITIONS.md — Dagster 1.13.10 asset graph + partition shapes

## 1. Asset declaration

```python
import dagster as dg

@dg.asset
def my_asset() -> dg.MaterializeResult:
    return dg.MaterializeResult(
        data_version=dg.DataVersion("v1"),
        metadata={"rows": 100},
    )
```

- **`AssetKey`** — the asset's identity; default `AssetKey(["my_asset"])`.
- **`MaterializeResult`** — what the body returns; carries `data_version`,
  `metadata`, `tags`. In 1.13.10 returning `None` is also accepted; Dagster
  auto-emits a placeholder materialization with an auto-computed
  `data_version` derived from input data_versions. The Execution Fabric
  exploits this: dispatch asset bodies return None; harvest sensor emits
  the real materialization later (`/WHITEPAPER.md` §3.2).

## 2. Asset dependencies

```python
@dg.asset
def upstream(): ...

@dg.asset(deps=[upstream])
def downstream(): ...

# Or by AssetKey:
@dg.asset(deps=[dg.AssetDep(dg.AssetKey("upstream"))])
def downstream2(): ...
```

`deps=` declares **lineage edges**, not input loading. To load upstream
artifacts use `ins={"x": dg.AssetIn("upstream")}` + an IO manager.

## 3. Partitions

Partition definitions:

| Class | Use for |
|---|---|
| `StaticPartitionsDefinition([...])` | Fixed enum (e.g. PVT corners) |
| `DailyPartitionsDefinition`, `HourlyPartitionsDefinition` | Time series |
| `DynamicPartitionsDefinition(name=...)` | Runtime-discovered (registered via `instance.add_dynamic_partitions(...)`) |
| `MultiPartitionsDefinition({dim: PartitionsDefinition})` | Cross-product |

```python
pd = dg.MultiPartitionsDefinition({
    "pvt":  dg.StaticPartitionsDefinition(["tt_25", "ff_125"]),
    "cell": dg.StaticPartitionsDefinition(["INV", "BUF"]),
})

@dg.asset(partitions_def=pd)
def characterize(context: dg.AssetExecutionContext):
    keys = context.partition_key.keys_by_dimension   # {"pvt": "tt_25", "cell": "INV"}
    ...
```

**Multi-partition key format**: `"<dim1_value>|<dim2_value>"` where
dimensions are ordered **alphabetically by dimension name**. So for
the above, the partition keys are `"INV|tt_25"`, `"INV|ff_125"`, etc.
(cell sorts before pvt). This is load-bearing in the Execution Fabric's
dispatch sensor — see `framework/sensor/dispatch.py` desired-set
computation.

## 4. Partition mappings

Default for same-shape upstream → downstream is **identity**. For
shape changes, declare a mapping explicitly:

| Mapping | What it does |
|---|---|
| `IdentityPartitionMapping()` | 1:1 same shape |
| `AllPartitionMapping()` | downstream depends on ALL upstream partitions |
| `LastPartitionMapping()` | downstream depends on most-recent upstream partition |
| `MultiToSingleDimensionPartitionMapping(partition_dimension_name=...)` | Upstream is single-dim; downstream is multi-partition; map a specific downstream dimension to the upstream's |
| `StaticPartitionMapping({"u1": ["d1"], …})` | Manual mapping table |

**`MultiToSingleDimensionPartitionMapping`** is the load-bearing one for
the Execution Fabric. Example: characterize is multi-partitioned by
`[pvt, cell]`; template_tcl is single-partitioned by `[pvt]`:

```python
@dg.asset(
    partitions_def=pd_multi,
    deps=[dg.AssetDep(
        dg.AssetKey("template_tcl"),
        partition_mapping=dg.MultiToSingleDimensionPartitionMapping(
            partition_dimension_name="pvt"
        ),
    )],
)
def characterize(): ...
```

**Beta warning**: 1.13.10 still emits `BetaWarning` for this class but
it's stable enough for production. The framework code uses it.

## 5. DataVersion + staleness

```python
@dg.asset
def src() -> dg.MaterializeResult:
    digest = sha256_of_input()
    return dg.MaterializeResult(data_version=dg.DataVersion(digest))
```

If a downstream's upstream `data_version` changes, the downstream
becomes **stale**. Dagster's `AutomaterializePolicy` / sensors can use
this to trigger reruns. The Execution Fabric uses upstream data_versions
as the **idempotency key's** trigger fingerprint
(`/WHITEPAPER.md §7.1`).

**Reading data_version from a sensor or asset body**:
```python
record = context.instance.get_latest_data_version_record(asset_key)
if record and record.data_version:
    dv = record.data_version.value
```

## 6. Definitions assembly

```python
import dagster as dg

defs = dg.Definitions(
    assets=[my_asset, characterize, ...],
    sensors=[dispatch_sensor, harvest_sensor],
    jobs=[dispatch_job, noop_job],
    executor=dg.in_process_executor,
)
```

`Definitions` is one per code location. `workspace.yaml` lists code
locations.

## 7. Gotchas seen in 1.13.10

1. **`define_asset_job` rejects mixed partition shapes in one selection.**
   Build one job per asset (or per shape).
2. **No `from __future__ import annotations` in modules with `@dg.asset`** —
   Dagster resolves `context: dg.AssetExecutionContext` by name at
   import; PEP-563 string annotations defeat that and raise
   `DagsterInvalidDefinitionError`.
3. **`MaterializeResult(data_version=DataVersion(...))`** — `data_version`
   takes a `DataVersion` instance, not a raw string.
4. **`asset_partitions_def_for_input` + `get_partition_keys_in_subset`**
   for reading the upstream's actually-loaded partition keys from an
   asset body.
