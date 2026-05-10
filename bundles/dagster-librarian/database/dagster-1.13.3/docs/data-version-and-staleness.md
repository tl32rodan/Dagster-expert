# `data_version` and staleness propagation

**Tested against Dagster 1.13.3.**

## What `data_version` is

A user-set string identifying the **output content** of an
asset materialization. Dagster compares the upstream's stored
`data_version` against what each downstream last consumed; if
mismatch → downstream is **stale** in the UI (yellow indicator).

Set via `MaterializeResult(data_version=DataVersion(<str>))`.
The string is opaque to Dagster — convention is a short hash
of the actual output content.

## Where it's stored

Two visible places (both public):

1. **Asset's Materializations tab** — `Data version` column
2. **Run's Events tab** — `ASSET_MATERIALIZATION` event's tags
   include `dagster/data_version: <value>`

The tag key in 1.13.3 is `dagster/data_version`. (See
`tags-and-versions.md` for old/new name history.)

## The constant-hash trap (most common production bug)

If a downstream asset's hash is computed from constants — i.e.
doesn't actually depend on upstream content — staleness chain
**silently breaks** at that node. Lineage graph still shows the
arrow, but UI says "fresh".

```python
# WRONG — mid_corner's hash never depends on raw_corner
@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    payload = b"mid_of:ff_125c"          # constant, ignores upstream
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()[:16]),
    )
```

After re-materializing raw_corner with new content, mid_corner
re-runs (correctly) but its `data_version` is **identical**.
Downstream of mid_corner therefore sees no change — never
goes stale, never re-runs. Production-incident territory.

## Two correct patterns

### Style A — function arg + IOManager (cleanest)

Dagster passes upstream's actual return value through the default
IOManager. Hash that:

```python
@asset
def raw_corner() -> MaterializeResult:
    payload = b"corner=ff_125c"
    return MaterializeResult(
        value=payload,                                    # IOManager stores
        data_version=DataVersion(_digest(payload)),
    )

@asset
def mid_corner(raw_corner: bytes) -> MaterializeResult:   # arg = upstream key
    output = b"mid_of:" + raw_corner                       # depends on upstream
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),         # propagates naturally
    )
```

Use when: assets pass values via Python objects. 100% public API.

### Style B — explicit deps + read upstream's actual output file

For EDA flows where assets write to filesystem, the natural
propagation is: downstream reads upstream's output file and
hashes it.

```python
@asset
def raw_corner() -> MaterializeResult:
    output_path = Path("/data/raw_corner_ff_125c.txt")
    payload = b"corner=ff_125c"
    output_path.write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"path": str(output_path)},
    )

@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    upstream_bytes = Path("/data/raw_corner_ff_125c.txt").read_bytes()  # read it
    output = b"mid_of:" + upstream_bytes                                 # use it
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),                       # hash own output
    )
```

Dagster doesn't see your filesystem; it just compares stored
`data_version`s. Filesystem does the propagation work.

## What NOT to do

### Don't query Dagster's metadata for upstream `data_version`

Tempting to look up upstream's `data_version` from the instance
and fold it into your hash:

```python
# DON'T DO THIS
from dagster._core.definitions.data_version import extract_data_version_from_entry  # _core import!

@asset(deps=[AssetKey("raw_corner")])
def mid_corner(context):
    event = context.instance.get_latest_materialization_event(AssetKey("raw_corner"))
    upstream_dv = extract_data_version_from_entry(event)  # private API
    ...
```

Two problems:
1. `dagster._core.*` is a private module path — breaks across
   Dagster minor versions without notice.
2. You're using Dagster metadata as a proxy for content. If the
   upstream's hash itself was wrong (constant-hash trap), you
   inherit the bug.

If you're tempted to do this, you actually want Style A
(function arg) or Style B (read the actual file).

## Diagnosing a broken propagation chain

Symptom: edit upstream → re-materialize → downstream-of-downstream
stays "fresh".

Check:
1. UI → middle asset's Materializations tab → did `Data version`
   actually change between two consecutive materializations?
2. If NO → middle asset's hash is computed from constants.
   That's the bug. Fix by Style A or Style B-with-files.
3. If YES → propagation is OK; the downstream-of-downstream
   probably hasn't been re-materialized yet (you only re-ran
   the middle, not the downstream).

## Related

- [`style-a-vs-b.md`](style-a-vs-b.md) — when to choose which
- [`tags-and-versions.md`](tags-and-versions.md) — `dagster/data_version` vs old `dagster/logical_version`
- Examples: [`02_style_a_chain.py`](../examples/02_style_a_chain.py),
  [`03_style_b_filesystem.py`](../examples/03_style_b_filesystem.py)
