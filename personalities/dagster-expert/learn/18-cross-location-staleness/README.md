# lab18 · cross-location staleness propagation

**Time**: 45 min · **Prerequisites**: lessons 07 (cross-location),
17 (cross-partition incremental)

> 💡 This lesson combines lessons 07 and 17: a downstream asset
> in code-location B depends on a partitioned upstream in code-
> location A, and we watch staleness flow across the boundary on
> a per-partition basis. The contract is identical to the
> single-location case — just demo'd across locations to confirm.

## What you'll learn

- Dagster's asset graph spans code locations; staleness does too.
- The wiring contract for cross-location partitioned deps:
  `deps=[AssetKey([...])]` + same/compatible partition definition.
- The "Day-7 federation bug" (`Error loading base asset job`) and
  how this lesson avoids it.
- Where cross-location incremental rerun helps in real AP work
  (one team owns `lib_lower`, another owns `lib_upper`).

## Setup

```bash
cd 18-cross-location-staleness
dagster dev -w workspace.yaml
# UI: http://127.0.0.1:3000
```

Two code locations:

| Location | Asset | Partitions |
|---|---|---|
| `lib_lower` | `lib_lower/kit_summary` | `corner`, `lvf`, `em`, `ht` |
| `lib_upper` | `lib_upper/signoff_report` | same 4 |

Cross-location dep is declared in `lib_upper`:

```python
@asset(
    key_prefix=["lib_upper"],
    partitions_def=branch_partitions,
    deps=[AssetKey(["lib_lower", "kit_summary"])],   # ← cross-loc
)
def signoff_report(context): ...
```

`lib_upper`'s `Definitions(assets=...)` lists ONLY `signoff_report`.
We do **not** declare an `AssetSpec` for the upstream — that is
the Day-7 trap from lesson 07.

## Walkthrough

### Step 1 · backfill everything

```bash
dagster asset materialize -w workspace.yaml --select lib_lower/kit_summary --partition corner,lvf,em,ht
dagster asset materialize -w workspace.yaml --select lib_upper/signoff_report --partition corner,lvf,em,ht
```

UI: lineage view spans both locations. Each asset shows a 4-cell
partition heatmap, all green.

### Step 2 · bump one upstream partition's content

Edit `lower/asset.py`:

```python
payload = f"kit_summary__{key}__rev=1".encode()   # change rev=1 to rev=2
```

Reload code locations in the UI (or restart `dagster dev`).

### Step 3 · re-materialize ONE partition of `lib_lower/kit_summary`

```bash
dagster asset materialize -w workspace.yaml --select lib_lower/kit_summary --partition corner
```

### Step 4 · observe selective staleness ACROSS locations

UI: open `lib_upper/signoff_report` partitions:

- `corner` — **stale** (yellow ↻)
- `lvf`, `em`, `ht` — still fresh

That's the contract: staleness crosses the code-location
boundary, but only flags the partition whose upstream actually
changed. The lineage edge does the work.

### Step 5 · re-materialize the stale upper partition

```bash
dagster asset materialize -w workspace.yaml --select lib_upper/signoff_report --partition corner
```

All 8 partitions across the 2 locations are fresh again.

## Why staleness propagates across locations

Dagster's asset graph is a **single graph**, regardless of how
many code locations contribute to it. The instance metadata
store records `(asset_key, partition_key) -> data_version` rows
for every materialization. When `lib_upper/signoff_report`'s
partition `corner` was last materialized, Dagster recorded the
`data_version` of `lib_lower/kit_summary` partition `corner` that
it consumed.

Re-materializing the upstream writes a new row. Dagster compares
the latest upstream version against what each downstream
partition recorded as "consumed" — across all locations. Mismatch
on `corner` → `corner` is stale.

## The Day-7 federation bug — why this lesson works

If you (incorrectly) declared the upstream as an `AssetSpec` in
`lib_upper`'s `Definitions(assets=[...])`, the implicit asset-job
builder in 1.13.3 would see two definitions for the same
`AssetKey(["lib_lower", "kit_summary"])` — one real
materializable asset in `lib_lower`, one spec in `lib_upper` —
and refuse to build the cross-location job:

```
Error loading base asset job
```

The fix is exactly what this lesson does: use ONLY
`deps=[AssetKey([...])]` on the downstream; no `AssetSpec`. See
`personalities/dagster-expert/database/dagster-1.13.3/docs/cross-location.md`
for the cheatsheet entry.

## What this means for real AP work

This pattern is how multi-team library boundaries work:

- Team A owns `lib_lower` — they maintain `kit_summary` and its
  upstream char flow. Their code location loads `lib_lower`.
- Team B owns `lib_upper` — they consume the `kit_summary` and
  produce sign-off artifacts. Their code location loads
  `lib_upper`.
- The platform team owns `workspace.yaml` — they declare both
  code locations and run the webserver.

When Team A re-materializes one corner of `kit_summary`, Team B
sees only that one signoff partition flagged stale. No
coordination meeting required, no manual notification, no full
backfill.

## Pitfalls

- **`signoff_report` partition is stale even though
  `kit_summary`'s same partition is still showing the old
  data_version** — code locations were not reloaded after the
  edit. Click "Reload all" in the UI or restart `dagster dev`.
- **`Error loading base asset job`** — see Day-7 trap above.
  Check `lib_upper`'s `Definitions(assets=...)` does not include
  an `AssetSpec` for the upstream.
- **Cross-location dep doesn't appear in lineage graph** — typo
  in the `AssetKey([...])`. Path elements must match the
  upstream's `key_prefix + asset_name` exactly. Here:
  `["lib_lower", "kit_summary"]`.
- **`lib_upper`'s partition def differs from `lib_lower`'s** —
  default `IdentityPartitionMapping` requires the partition
  definitions to be equal (same keys, same type). If they differ
  in any way you must wire them explicitly with
  `StaticPartitionMapping` (lesson 17b's pattern), or staleness
  won't propagate cleanly.
- **Single-process `python_module:` workspace fails in
  production** — fine for this lesson, but the production
  pattern is `grpc_server:` so each location runs in its own
  Python process with its own deps. See
  `personalities/dagster-expert/skills/workspace-yaml-reference/SKILL.md`.

## What to try next

→ **Lesson 19** — same incremental promise, but instead of
clicking "Materialize" by hand, an `AutoMaterializePolicy.eager()`
makes the daemon do it automatically when upstream partitions
change.
