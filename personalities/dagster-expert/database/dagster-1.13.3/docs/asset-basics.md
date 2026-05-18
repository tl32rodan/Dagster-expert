# Asset basics — `@asset`, `MaterializeResult`, `DataVersion`

**Tested against Dagster 1.13.3.**

## Smallest possible asset

```python
import hashlib
from dagster import DataVersion, Definitions, MaterializeResult, asset


@asset
def greeting() -> MaterializeResult:
    payload = b"hello, dagster"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"size_bytes": len(payload), "preview": payload.decode()},
    )


defs = Definitions(assets=[greeting])
```

Run:
```bash
dagster dev -m my_module        # if Definitions is at top of module
```

Or for a single-file:
```bash
dagster dev -f my_file.py
```

## `MaterializeResult` — public fields

```python
MaterializeResult(
    asset_key=None,            # required only in @multi_asset
    metadata=None,             # dict[str, Any] — surfaces in UI events
    check_results=None,        # for @asset_check
    data_version=None,         # DataVersion — see data-version-and-staleness.md
    tags=None,                 # dict[str, str] — extra event tags
    value=NoValueSentinel,     # The output value; IOManager stores it
)
```

`value` enables Style A propagation (see `style-a-vs-b.md`). When
omitted, no value is stored (suitable for Style B / filesystem
flows).

## `DataVersion`

```python
DataVersion(value: str)
```

The value is opaque to Dagster — convention is a short hash of
the asset's output content. Used for staleness detection
downstream.

## `Definitions(...)` — module entry point

```python
defs = Definitions(
    assets=[asset1, asset2, ...],
    resources={"name": resource_def},
    schedules=[...],
    sensors=[...],
)
```

`Definitions` is the only object Dagster looks for at module
load time. Make it a top-level name (`defs = Definitions(...)`).

## `@asset` decorator — common parameters

```python
@asset(
    name="explicit_name",                    # default: function name
    key_prefix=["lib_lower"],                # for cross-location organization
    deps=[AssetKey("upstream"), ...],        # Style B deps
    partitions_def=corner_partitions,        # see partitions.md
    code_version="v1",                       # opt-in; default None.
                                             # The only way to make reload (without re-materialize)
                                             # flag downstream as stale — see data-version-and-staleness.md.
    retry_policy=RetryPolicy(max_retries=3), # see failures-retries.md
    metadata={...},                          # static metadata (vs MaterializeResult.metadata)
    tags={...},                              # static tags
    description="...",                       # surfaced in UI
)
def my_asset(context, config, input1) -> MaterializeResult: ...
```

## Asset function signature

```python
@asset
def my_asset(
    context: AssetExecutionContext,    # optional; gets you logging, partition_key, instance
    config: MyConfig,                  # optional; see runconfig.md
    upstream_key: bytes,               # Style A dep — arg name = upstream
) -> MaterializeResult:
    ...
```

All args optional. Return type can be:
- `MaterializeResult` (recommended)
- The output value directly (auto-wrapped; less control)
- `Output(value, metadata=...)` (older API, still works)

## Common gotchas

- **`from __future__ import annotations` breaks decoration** — see
  `future-annotations-incompat.md`
- **`@asset` body that ignores upstream value but uses Style B**
  → silent staleness propagation break — see
  `data-version-and-staleness.md`
- **`Definitions(...)` at submodule level not picked up** — must
  re-export from `__init__.py` (`from .asset import defs`) for
  `dagster dev -m <pkg>` to find it; or use `dagster dev -m <pkg>.<sub>` /
  `dagster dev -f <path>`

## Related

- [`style-a-vs-b.md`](style-a-vs-b.md) — declaring deps
- [`data-version-and-staleness.md`](data-version-and-staleness.md)
  — propagation contract
- [`partitions.md`](partitions.md) — `partitions_def`
- [`runconfig.md`](runconfig.md) — `config:` arg
- Examples: [`01_basic_asset.py`](../examples/01_basic_asset.py)
