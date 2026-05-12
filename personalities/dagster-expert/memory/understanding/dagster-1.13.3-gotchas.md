# Dagster 1.13.3 — API gotchas discovered while building scale-lib demo

Reference for future Dagster 1.13.3 work in this corpus. Each entry has
a one-line symptom, the cause, and the fix. Verified during the
2026-05-12 scale-lib build (89/89 tests pass).

## 1. ``from __future__ import annotations`` breaks ``@asset`` context validation

**Symptom**: `DagsterInvalidDefinitionError: Cannot annotate \`context\`
parameter with type AssetExecutionContext. \`context\` must be annotated
with AssetExecutionContext, ... or left blank.`

**Cause**: With future annotations, the type annotation becomes a string
literal at runtime. Dagster's `_validate_context_type_hint` reads the
literal and rejects it (even though the string IS "AssetExecutionContext").

**Fix**: In any file that defines `@asset`-decorated functions, do NOT
use `from __future__ import annotations`. Leave context annotation as
the real class. Other files (spec/, rules/) can still use future
annotations freely.

## 2. ``@asset(key=AssetKey([...]))`` conflicts with ``name`` / ``key_prefix``

**Symptom**: `DagsterInvalidDefinitionError: Cannot specify a name or
key prefix for @asset when the key argument is provided.`

**Fix**: Use either-or:
- Single AssetKey: `@asset(key=AssetKey([lib, step]))`
- Split: `@asset(key_prefix=[lib], name=step)` ← lesson 11 pattern

## 3. ``ins={...}`` triggers upstream IO loading

**Symptom**: `FileNotFoundError` when Dagster's default IOManager
tries to load an upstream partition's output from `.../storage/lib_a/step0/corner`.

**Cause**: `ins=` declares an actual input that Dagster passes into the
asset body via the IOManager. For folder-as-asset / Style B / Pipes
patterns, the body never reads the upstream's *returned value*; only
the *folder on disk*.

**Fix**: Use `deps=[AssetDep(asset=key, partition_mapping=...)]` instead.
`AssetDep` carries the partition mapping for lineage / staleness but
doesn't force IO loading.

```python
from dagster import AssetDep, AssetKey
deps = [AssetDep(asset=AssetKey([lib, "step4"]),
                 partition_mapping=IdentityPartitionMapping())]
```

## 4. Custom ``PartitionMapping`` subclasses are deprecated for reconciliation

**Symptom**: `DeprecationWarning: Non-built-in PartitionMappings, such
as _MyMapping are deprecated and will not work with asset reconciliation.`

**Cause**: Auto-materialize / reconciliation only knows the built-in
mappings: `AllPartitionMapping`, `IdentityPartitionMapping`,
`LastPartitionMapping`, `SpecificPartitionsPartitionMapping`,
`StaticPartitionMapping`, `TimeWindowPartitionMapping`,
`MultiToSingleDimensionPartitionMapping`, `MultiPartitionMapping`.

**Fix**: For static partition sets, pre-compute a `StaticPartitionMapping`
by enumerating all branches at definition time:

```python
from dagster import StaticPartitionMapping
from collections import defaultdict

up_to_down: dict[str, set[str]] = defaultdict(set)
for downstream in all_branches():
    for upstream in rule.resolve(downstream):
        up_to_down[upstream].add(downstream)
return StaticPartitionMapping(
    downstream_partition_keys_by_upstream_partition_key={
        up: sorted(downs) for up, downs in up_to_down.items()
    },
)
```

Keep the custom adapter as a fallback for dynamic / future cases.

## 5. ``PartitionsSubset`` not exposed at top level

**Symptom**: `ImportError: cannot import name 'PartitionsSubset' from 'dagster'`

**Fix**: Import from internal path (acceptable for typing / adapter
implementation — won't break across 1.13.x patch releases):

```python
from dagster._core.definitions.partitions.subset.partitions_subset \
    import PartitionsSubset
from dagster._core.definitions.partitions.mapping.partition_mapping \
    import UpstreamPartitionsResult
```

## 6. ``Definitions.get_asset_graph()`` and ``.all_asset_keys`` renamed

**Symptom**: `AttributeError: 'Definitions' object has no attribute
'get_asset_graph'. Did you mean: 'resolve_asset_graph'?`

**Fix**:
- `defs.resolve_asset_graph()` returns `AssetGraph`
- `asset_graph.get_all_asset_keys()` (method) — NOT `.all_asset_keys` property

## 7. ``AssetKey.path`` returns a list (unhashable)

**Symptom**: `TypeError: unhashable type: 'list'` when doing
`{k.path for k in asset_keys}`.

**Fix**: `{tuple(k.path) for k in ...}`.

## 8. PartitionMapping abstract method is ``get_upstream_mapped_partitions_result_for_partitions``

The 1.13.3 public abstract for custom subclasses (rare; prefer built-ins
per #4). Signature:

```python
def get_upstream_mapped_partitions_result_for_partitions(
    self,
    downstream_partitions_subset: PartitionsSubset | None,
    downstream_partitions_def: PartitionsDefinition | None,
    upstream_partitions_def: PartitionsDefinition,
    current_time: datetime | None = None,
    dynamic_partitions_store=None,
) -> UpstreamPartitionsResult: ...

def get_downstream_partitions_for_partitions(
    self,
    upstream_partitions_subset: PartitionsSubset,
    upstream_partitions_def: PartitionsDefinition,
    downstream_partitions_def: PartitionsDefinition,
    current_time: datetime | None = None,
    dynamic_partitions_store=None,
) -> PartitionsSubset: ...
```

Return `UpstreamPartitionsResult(partitions_subset=..., required_but_nonexistent_subset=...)`.

## 9. GraphQL ``AssetNode`` source-asset shape

**Fields that actually exist** (verified via introspection):
- `isObservable: bool`     — true for `@observable_source_asset`
- `isMaterializable: bool` — false for source assets
- NOT `isSource` (that name doesn't exist in 1.13.3 schema)

For UI integration tests, query `isObservable` + `isMaterializable` to
distinguish source vs regular assets.

## 10. ``dagster.yaml`` location

`dagster dev -w workspace.yaml` warns if `dagster.yaml` is in CWD but
`$DAGSTER_HOME` points elsewhere. The yaml must be inside `$DAGSTER_HOME`
to actually be loaded. The CWD copy is silently ignored.

Bootstrap incantation:

```bash
mkdir -p "$DAGSTER_HOME"
cp dagster.yaml "$DAGSTER_HOME/dagster.yaml"
```

## 11. ``observable_source_asset`` emits a beta warning

`BetaWarning: Function observable_source_asset is currently in beta`.
Acceptable in 1.13.3; no replacement exists for change-event propagation
from non-asset sources. Filter from CI test output via pytest's
`filterwarnings = ignore::dagster.BetaWarning`.
