# Cross-location dependencies + Day-7 federation bug

**Tested against Dagster 1.13.3.**

## What a "code location" is

One Python process loading one `Definitions(...)` object. The
webserver can load multiple code locations from one
`workspace.yaml`, and the asset graph spans all of them.

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

(Or `grpc_server: { host: ..., port: ... }` for production —
see `dagster-operator/workspace-yaml-reference`.)

## Cross-location asset dependency — the right way

Downstream uses `deps=[AssetKey([...])]` to point at the
upstream key. **Upstream key is just a key** — Dagster looks
across all loaded locations.

```python
# upper/asset.py
from dagster import asset, AssetKey, MaterializeResult, Definitions

@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],   # cross-loc dep
)
def signoff_report() -> MaterializeResult: ...

defs = Definitions(assets=[signoff_report])          # ← only own assets here
```

## ⚠ The Day-7 federation bug

Real case from Brian's LENS PoC, May 2026: cross-location asset
job failed to load with:

```
Error loading base asset job
```

**Cause**: downstream code location declared an `AssetSpec` for
the upstream key AND included it in its `Definitions(assets=...)`:

```python
# WRONG — Day-7 broken pattern
from dagster import AssetSpec

external_lower_kit_summary = AssetSpec(
    key=AssetKey(["lib_lower", "kit_summary"]),
)

@asset(deps=[AssetKey(["lib_lower", "kit_summary"])])
def signoff_report(): ...

defs = Definitions(
    assets=[external_lower_kit_summary, signoff_report],   # ← THE BUG
)
```

The same `AssetKey` is now defined as a real asset in `lib_lower`
AND as an `AssetSpec` in `lib_upper`'s asset list. Dagster
1.13.3's implicit asset-job builder sees two definitions for the
same key and refuses.

**Fix**: drop `AssetSpec` from `Definitions(assets=...)`. Use
**only** `deps=[AssetKey([...])]` on the downstream asset.

## Verifying cross-location wiring

```bash
dagster definitions list -w workspace.yaml         # all locations
dagster definitions validate -w workspace.yaml     # any errors?
dagster asset list -w workspace.yaml               # full graph spans locations
```

## Common gotchas

- **"Could not load location: Connection refused"** — gRPC code
  server not running or wrong host/port. `nc -zv host port`.
- **"ModuleNotFoundError"** — webserver venv missing the package
  in `python_module` mode. Either install it or switch to gRPC.
- **"Error loading base asset job"** — Day-7 trap above.
- **Cross-location dep doesn't appear in graph** — typo in
  `AssetKey([...])`. Path elements must match the upstream's
  `key_prefix` + asset name exactly.

## When to split locations

- Different teams own different parts of the DAG
- One library needs a different `dagster` minor version
- One library has heavy native deps you don't want in the rest
- Failure isolation: bad code in one library doesn't break others

Don't split for organization alone — `key_prefix` and groups
within one location are usually enough.

## Related

- [`asset-basics.md`](asset-basics.md) — `@asset` + `Definitions`
- `dagster-operator/workspace-yaml-reference` — gRPC code-server
  production pattern
- `dagster-operator/diagnose-codeloc-fail` — runbook for "Error
  loading base asset job"
- Examples: [`08_cross_location_workspace/`](../examples/08_cross_location_workspace/)
