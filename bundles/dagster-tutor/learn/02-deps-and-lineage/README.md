# lab2 · deps + lineage — chaining assets

**Time**: 30 min · **Prerequisites**: lesson 01

## What you'll learn

- How to declare an asset that depends on another asset
- What "lineage" looks like in the Dagster UI
- The data-version → staleness chain
- The difference between explicit `deps=[...]` and inferred
  function-arg dependencies

## The lesson in 60 seconds

```bash
cd 02-deps-and-lineage
dagster dev -m chain
# open http://127.0.0.1:3000
```

You should see three assets in a chain: `raw_corner` → `mid_corner`
→ `final_corner`. The Assets graph view shows them as nodes with
arrows.

Click **Materialize all**. All three run in topological order.

## What's actually happening

Dagster figures out **which order to run things** from the
declared deps. You don't write a job; you describe the data graph,
and Dagster builds the job.

Two equivalent ways to declare a dep:

```python
# Style A — implicit (function arg name = upstream key)
@asset
def mid_corner(raw_corner) -> MaterializeResult:
    ...

# Style B — explicit, with deps=[...]
@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    ...
```

A is convenient when you can pass the data through Python.
B is mandatory when the dep is just for ordering — e.g., the
upstream asset wrote to a shared filesystem and the downstream
reads from there directly. **Most TSMC EDA pipelines use Style B**
because their tools write to disk, not to Python objects.

## Now try

### Try 1 · Click Materialize on just `final_corner`

Dagster will materialize **only** `final_corner`. The upstream
two are not re-run, because Dagster trusts their stored data
versions are still current.

> Why: you didn't say "and re-run everything upstream". Dagster's
> default is to honor your selection.

### Try 2 · Edit `raw_corner`'s payload, then Materialize `final_corner` only

Same thing — Dagster materializes only `final_corner`. But the
**lineage view now shows `final_corner` as STALE** (yellow dot).

> Why: `raw_corner`'s data version changed; `final_corner`'s
> stored materialization references the OLD version. Dagster
> can't auto-decide whether you wanted to refresh — but it tells
> you something is out of date.

### Try 3 · Materialize the stale chain

Click Materialize on `final_corner` with the "include upstream"
checkbox (or `--select +final_corner` in CLI). Now `raw_corner`
and `mid_corner` re-run too.

```bash
dagster asset materialize -w workspace.yaml --select '+final_corner'
```

> The `+` prefix means "and everything upstream of this".

## ⚠ The data_version-doesn't-propagate trap (production hazard)

`data_version` is your contract for "did the output change?". If
a downstream asset's `data_version` is computed from constants
(or anything that doesn't depend on upstream's content),
staleness propagation **silently breaks** at that node. Lineage
graph still draws the arrow; the chain looks fine. But change
upstream and re-materialize — downstream-of-downstream stays
"fresh" because the middle node's data_version never moved.

This file's earlier version had this bug. Spot the difference:

```python
# WRONG — mid_corner's hash never depends on raw_corner
@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    payload = b"mid_of:ff_125c"          # constant!
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()[:16]),
    )

# RIGHT — mid_corner's hash folds in upstream's data_version
@asset(deps=[AssetKey("raw_corner")])
def mid_corner(context) -> MaterializeResult:
    raw_dv = _upstream_data_version(context, AssetKey("raw_corner"))
    payload = f"mid_of:{raw_dv}".encode()    # depends on upstream
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()[:16]),
    )
```

In EDA flows, the natural fix is: hash the **upstream's actual
output file** (the `.lib` / `.lef` / report file). Upstream
content → upstream hash → downstream hash. Filesystem does the
work. Dagster's role is just to track and compare. This file
uses upstream's `data_version` as a stand-in (since the lesson
doesn't actually write files); same idea.

**Style A doesn't have this problem.** When the function takes
`raw_corner: bytes`, Dagster passes the actual upstream value
through the IOManager — your hash naturally depends on upstream's
content because you're literally hashing it. Style B (explicit
deps) puts the propagation contract on YOU.

## Common pitfalls

- **No arrows in the graph**: you used `deps=[...]` with a string
  but the upstream asset's key has a prefix. Use `AssetKey([...])`
  with the full key path.
- **"Asset X has no upstream"**: the function arg name doesn't
  match the upstream asset's key, AND you didn't add `deps=`.
  Either rename or add `deps=`.
- **Materialize re-runs everything**: you used `--select '*'` or
  the UI's "Materialize all" button. Use `+asset` for upstream-of
  or `asset+` for downstream-of.

## Cheat sheet

```python
from dagster import asset, AssetKey, MaterializeResult, DataVersion

# Implicit dep via arg name
@asset
def downstream(upstream): ...

# Explicit deps (the TSMC EDA pattern)
@asset(deps=[AssetKey("upstream")])
def downstream() -> MaterializeResult: ...

# Multiple deps
@asset(deps=[AssetKey("a"), AssetKey("b")])
def joined(): ...

# Upstream key with prefix
@asset(deps=[AssetKey(["lib_lower", "kit_summary"])])
def downstream(): ...
```

CLI selectors:
- `asset` — just that asset
- `asset+` — that asset and all downstream
- `+asset` — that asset and all upstream
- `++asset` — and 2 hops upstream
