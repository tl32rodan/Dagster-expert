# Style A vs Style B — declaring asset dependencies

**Tested against Dagster 1.13.3.**

Two ways to express "asset B depends on asset A". Same lineage
graph in the UI; different runtime semantics; different
propagation behavior.

## Style A — function arg matches upstream key

```python
@asset
def raw_corner() -> MaterializeResult:
    return MaterializeResult(value=b"...", data_version=...)

@asset
def mid_corner(raw_corner: bytes) -> MaterializeResult:
    #            ^^^^^^^^^^ arg name = upstream key → implicit dep
    return MaterializeResult(value=..., data_version=...)
```

**Runtime**: Dagster's default IOManager pickles `raw_corner`'s
`MaterializeResult.value` and feeds it back to `mid_corner` as
the function argument.

**Use when**: you want upstream's value flowed through Python.
`data_version` propagation is automatic — your hash naturally
depends on what you receive.

**Limitations**:
- Default IOManager is local pickle; for large values, configure
  a different IOManager (e.g. S3, MinIO).
- Cross-host execution requires shared filesystem or remote
  IOManager.
- All upstream values must be pickle-able.

## Style B — explicit `deps=[AssetKey(...)]`

```python
@asset
def raw_corner() -> MaterializeResult:
    Path("/data/raw.txt").write_bytes(b"...")
    return MaterializeResult(data_version=...)

@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    upstream = Path("/data/raw.txt").read_bytes()       # YOU read upstream
    output = transform(upstream)
    return MaterializeResult(data_version=DataVersion(_digest(output)))
```

**Runtime**: Dagster only knows about ordering and
materialization events. Upstream's value never reaches downstream
unless YOU read it (typically from a known filesystem location).

**Use when**:
- Upstream wrote to disk / SOS / external system
- Downstream is a separate tool invocation (e.g. EDA binary) that
  reads file paths, not Python objects
- Cross-host without shared IOManager

**Propagation responsibility is yours**: hash your own output
(after reading upstream) — Dagster sees nothing about upstream's
content, only the `data_version` you set.

## Side-by-side

| Question | Style A | Style B |
|---|---|---|
| How upstream value reaches downstream | IOManager (default: pickle to disk) | You read it from a known location |
| `data_version` propagation | Automatic if you hash your output | Manual; YOU must hash content that depends on upstream |
| Across hosts | Need shared IOManager | Need shared filesystem (or none, if downstream is single-host) |
| Asset returns `MaterializeResult.value=...` needed? | Yes (so IOManager has something to store) | No (downstream doesn't read this) |
| TSMC EDA fit | Possible if values fit Python | Default fit — EDA tools read files |

## Common mistake: Style B with constant hash

```python
@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    payload = b"mid_of:ff_125c"                    # constant — IGNORES upstream
    return MaterializeResult(data_version=DataVersion(_digest(payload)))
```

`mid_corner` correctly re-runs after upstream changes (Dagster
respects the dep), but its `data_version` is **identical** every
time. Downstream-of-mid never sees a change → propagation chain
**silently broken**.

Fix: make the hash depend on upstream — either read upstream's
file (Style B) or switch to Style A (function arg).

See [`data-version-and-staleness.md`](data-version-and-staleness.md)
for the full discussion.

## Mixing the two styles

You can mix per-asset:

```python
@asset
def raw_corner() -> MaterializeResult: ...           # Style A producer

@asset
def mid_python(raw_corner: bytes): ...               # Style A consumer

@asset(deps=[AssetKey("raw_corner")])
def mid_eda(): ...                                   # Style B consumer (reads file)
```

Both `mid_python` and `mid_eda` depend on `raw_corner`; choose
per asset.

## Related

- [`asset-basics.md`](asset-basics.md) — `@asset` / `MaterializeResult`
- [`data-version-and-staleness.md`](data-version-and-staleness.md) — propagation contract
- Examples: [`02_style_a_chain.py`](../examples/02_style_a_chain.py),
  [`03_style_b_filesystem.py`](../examples/03_style_b_filesystem.py)
