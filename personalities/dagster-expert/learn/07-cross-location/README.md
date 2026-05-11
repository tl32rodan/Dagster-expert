# lab7 · cross-location dependencies

**Time**: 30 min · **Prerequisites**: lessons 01, 02

## What you'll learn

- What a "code location" is in Dagster
- Why you'd split a project across multiple locations
- How to depend on an asset that lives in another code location
- The **Day-7 federation bug** — a real trap that breaks
  multi-location asset jobs (and how to avoid it)

## Why split into multiple code locations

In a single-team project, one location is fine. You split when:

- Different teams own different parts of the DAG (lib_lower vs
  lib_upper at TSMC)
- One library needs a different `dagster` minor version
- One library has heavy native deps you don't want in the rest of
  the deploy
- You want failure isolation: bad code in `lib_upper` shouldn't
  prevent `lib_lower` from loading

Each code location is one Python process loading one
`Definitions(...)` object. They communicate over gRPC; the
webserver stitches them into one asset graph.

## The lesson in 60 seconds

```bash
cd 07-cross-location
dagster dev -w workspace.yaml
# open http://127.0.0.1:3000
```

`workspace.yaml` declares two code locations:

```yaml
load_from:
  - python_module:
      module_name: lower
      location_name: lib_lower
  - python_module:
      module_name: upper
      location_name: lib_upper
```

The Assets graph shows both locations stitched together:
`lib_lower/kit_summary` → `lib_upper/signoff_report`. Click
"Materialize all". Both run; the upper waits for the lower.

## How the dependency is wired

The downstream asset uses `deps=[AssetKey([...])]` to point at
the upstream key. The upstream key is **just a key** — Dagster
looks across all loaded locations to find a matching real asset.

```python
# upper/asset.py — depends on a key from lib_lower
@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],
)
def signoff_report() -> MaterializeResult:
    ...

defs = Definitions(assets=[signoff_report])
```

Notice: `lib_upper`'s `Definitions` lists ONLY its own asset.
It does NOT include any spec or stub for `lib_lower`'s asset.

## ⚠️ The Day-7 federation bug — DO NOT DO THIS

Real case from the LENS PoC (Day 7, May 2026): cross-location
asset jobs failed to load with the cryptic error:

```
Error loading base asset job
```

**Root cause**: the downstream code location declared an
`AssetSpec` for the upstream key AND included it in its
`Definitions(assets=...)` list:

```python
# lib_upper/assets.py — BROKEN
from dagster import AssetSpec

external_lower_kit_summary = AssetSpec(
    key=AssetKey(["lib_lower", "kit_summary"]),
)

@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],
)
def signoff_report(): ...

defs = Definitions(
    assets=[external_lower_kit_summary, signoff_report],   # ← the bug
)
```

The same `AssetKey` is now defined as a real asset in `lib_lower`
AND as an `AssetSpec` in `lib_upper`'s asset list. Dagster
1.13.3's implicit-asset-job builder sees two definitions for the
same key and refuses to build the job. The error surfaces at
webserver load time as "Error loading base asset job".

**Fix**: drop the `AssetSpec` from `Definitions(assets=...)`. Use
**only** `deps=[AssetKey([...])]` on the downstream asset. The
working code in this lab implements the fix.

## Now try

### Try 1 · Materialize the upper without the lower

Click `signoff_report` in the UI. Materialize. Look at the run —
it materializes only the upper, but the lineage view shows it as
"depends on stale or missing upstream" because `kit_summary` was
never materialized.

Now materialize `kit_summary` first, then `signoff_report`. Clean
green chain.

### Try 2 · Reproduce the Day-7 bug

Edit `upper/asset.py`:

```python
from dagster import AssetSpec

external_lower_kit_summary = AssetSpec(
    key=AssetKey(["lib_lower", "kit_summary"]),
)

defs = Definitions(
    assets=[external_lower_kit_summary, signoff_report],
)
```

Reload the code location. The UI shows a red banner:
"Error loading base asset job". Run
`dagster definitions validate -w workspace.yaml` to see the full
trace.

Revert the change, reload — back to green.

### Try 3 · Restart only one code location

`dagster dev` reloads everything. In production you'd restart only
the failing code server; the other location keeps running. To see
that locally, use two terminals — one running each code-server,
and have the webserver point at both. (See `dagster-operator`'s
`workspace-yaml-reference` skill for the gRPC pattern.)

## Common pitfalls

- **"Could not load location: Connection refused"**: gRPC code
  server not running, or wrong host/port in `workspace.yaml`.
- **"ModuleNotFoundError: lib_lower"**: the webserver's venv
  doesn't have `lib_lower` installed. In `python_module` mode,
  every location module must be importable by the loading process.
- **Cross-location dep doesn't appear in graph**: typo in
  `AssetKey([...])` — the path elements must match the upstream
  asset's `key_prefix` + asset name exactly.
- **"Error loading base asset job"**: see the Day-7 bug above.

## Cheat sheet

```python
# upstream code location (lib_lower)
from dagster import asset, MaterializeResult, Definitions

@asset(key_prefix=["lib_lower"])
def kit_summary() -> MaterializeResult: ...

defs = Definitions(assets=[kit_summary])
```

```python
# downstream code location (lib_upper)
from dagster import asset, AssetKey, MaterializeResult, Definitions

@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],
)
def signoff_report() -> MaterializeResult: ...

defs = Definitions(assets=[signoff_report])   # ← only own asset
```

```yaml
# workspace.yaml
load_from:
  - python_module:
      module_name: lower
      location_name: lib_lower
  - python_module:
      module_name: upper
      location_name: lib_upper
```

## See also

- `dagster-operator` skill `workspace-yaml-reference` for the
  gRPC code-server production pattern
- `dagster-operator` skill `diagnose-codeloc-fail` (Symptom D) for
  the Day-7 bug as a runbook entry
