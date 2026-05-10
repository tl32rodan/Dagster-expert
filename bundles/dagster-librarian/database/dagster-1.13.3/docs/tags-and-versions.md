# Tag keys + version-renamed constants

**Tested against Dagster 1.13.3.**

Dagster stores asset-materialization metadata as **tags** on the
materialization event. The tag KEYS are part of the public
contract (visible in UI, queryable from event log) but Dagster
has renamed them across versions.

## Current (1.13.3) canonical tag keys

| Tag key | What it carries |
|---|---|
| `dagster/data_version` | The user-set or auto-computed `DataVersion` for the asset's output |
| `dagster/data_version_is_user_provided` | `"true"` if user passed `data_version=DataVersion(...)`, `"false"` if Dagster auto-computed |
| `dagster/code_version` | The asset function's code version (auto-hashed from source, or `@asset(code_version="...")` if user set) |
| `dagster/input_data_version/<asset_key>` | Per-input data_version this materialization observed |
| `dagster/input_event_pointer/<asset_key>` | Storage ID of the input event used |

## Renamed tags (don't use the old names)

| Pre-1.13 | 1.13+ |
|---|---|
| `dagster/logical_version` | `dagster/data_version` |
| `dagster/input_logical_version/<key>` | `dagster/input_data_version/<key>` |

Dagster keeps the old names as `_OLD_DATA_VERSION_TAG` /
`_OLD_INPUT_DATA_VERSION_TAG_PREFIX` for backwards compat reading,
but **never write** the old names.

## Public Python access (don't reach into `_core`)

The constants live in `dagster._core.definitions.data_version` —
that's a **private** path. Don't import from there.

For reading data_version from a materialization event, prefer:

```python
event = context.instance.get_latest_materialization_event(asset_key)
mat = event.dagster_event.event_specific_data.materialization
tags = mat.tags or {}
dv_str = tags.get("dagster/data_version")     # hard-code key name
```

The tag key itself isn't exported as a public constant in 1.13.3,
so hard-coding the string is the least-bad option for Style B
flows. (Discouraged anyway — see `data-version-and-staleness.md`
for why this whole pattern is suspect.)

## How users see these tags in the UI

Asset detail page → **Materializations** tab → click a row →
**Events** sub-tab → look at `tags` in the event payload.

Or: Run details page → event log → `ASSET_MATERIALIZATION` event →
expand → `tags` field.

## When the tag is missing

Two reasons `dagster/data_version` could be absent from a
materialization's tags:

1. The asset was materialized in a Dagster version pre-1.13 and
   the tag is stored under the old name `dagster/logical_version`.
   Read both keys for backward compat.
2. The asset didn't set `data_version` at all (no `MaterializeResult`
   or `MaterializeResult(data_version=None)`) AND Dagster's
   auto-version computation wasn't enabled. Tag absent = no version
   stored.

## Related

- [`data-version-and-staleness.md`](data-version-and-staleness.md)
- [`avoid-private-imports.md`](avoid-private-imports.md)
