# Dagster 1.13.3 cheatsheet — INDEX

Curated reference for Brian's air-gap deployment. Each entry is
focused on **what to write** and **what NOT to write**, with
runnable examples in `../examples/`.

## Entries

> **START HERE** for any "how do I run / operate / structure things" question.
> [`STANDARD_USAGE.md`](STANDARD_USAGE.md) is the canonical golden path; the
> librarian answers usage/architecture questions ONLY from it (no SMAK). The
> entries below are API-level cheatsheets used for raw signature lookups.

| Topic | File | When you'd ask it |
|---|---|---|
| **Standard usage (the ONE prescribed path)** | [`STANDARD_USAGE.md`](STANDARD_USAGE.md) | "what's the standard / recommended way?" / "architecture, daemon, triggers, UI vs CLI, which knob?" |
| Asset basics: `@asset`, `MaterializeResult`, `DataVersion` | [`asset-basics.md`](asset-basics.md) | "what's the smallest @asset?" / "where do I set data_version?" |
| Style A vs Style B (deps via fn arg vs explicit) | [`style-a-vs-b.md`](style-a-vs-b.md) | "function arg vs `deps=[]`?" / "how does upstream value reach my asset?" |
| `data_version` propagation + the constant-hash trap | [`data-version-and-staleness.md`](data-version-and-staleness.md) | "why doesn't downstream go stale when upstream changes?" |
| Partitions: static, dynamic, multi (1.13.3 limits) | [`partitions.md`](partitions.md) | "how do I parameterize an asset by corner?" / "MultiPartitions limit?" |
| Run config + `Config` class (Pydantic) | [`runconfig.md`](runconfig.md) | "how do I parameterize a single run?" |
| Failures, retries, run states | [`failures-retries.md`](failures-retries.md) | "what's `RetryPolicy`?" / "stuck STARTED runs?" |
| Cross-location dependencies + Day-7 federation bug | [`cross-location.md`](cross-location.md) | "two code locations, one depends on the other" / "Error loading base asset job" |
| Tag keys + version-renamed constants | [`tags-and-versions.md`](tags-and-versions.md) | "what's the tag for data_version?" / "old name vs new name?" |
| `from __future__ import annotations` incompatibility | [`future-annotations-incompat.md`](future-annotations-incompat.md) | "why does my @asset reject `context: AssetExecutionContext`?" |
| Air-gap API discovery technique | [`api-discovery-offline.md`](api-discovery-offline.md) | "how do I find the right Dagster API without internet?" |
| `_core.*` private modules — what NOT to use | [`avoid-private-imports.md`](avoid-private-imports.md) | "I see `from dagster._core import X` — is that OK?" |

## Examples (`../examples/`)

| File | Demonstrates |
|---|---|
| `01_basic_asset.py` | Smallest `@asset` with `MaterializeResult` |
| `02_style_a_chain.py` | Function-arg deps, IOManager value flow, automatic propagation |
| `03_style_b_filesystem.py` | Explicit deps + downstream reads upstream's output FILE |
| `04_partitioned.py` | StaticPartitionsDefinition |
| `05_multipartition_2d.py` | MultiPartitions (2D limit in 1.13.3) |
| `06_runconfig.py` | Pydantic `Config` class + `MaterializeResult` |
| `07_retry_policy.py` | `RetryPolicy(max_retries=3, backoff=Backoff.EXPONENTIAL)` |
| `08_cross_location_workspace/` | Two code locations + correct cross-loc dep |

## Smoke-test the corpus

```bash
source ~/dagster-venv/bin/activate
cd personalities/dagster-expert/database/dagster-1.13.3/examples
for d in $(ls -d */) NA; do                      # multi-loc workspaces
  [ "$d" = "NA" ] && continue
  ( cd "$d" && dagster definitions validate -w workspace.yaml 2>&1 | tail -1 )
done
for f in *.py; do
  pkg="${f%.py}"
  ( cd . && dagster definitions validate -f "$f" 2>&1 | tail -1 )
done
```

If any line is not "All code locations passed validation.", the
example has rotted under the current Dagster version. Fix or
quarantine.

## How consumers use this corpus

**Direct file read** (when topic is known):
```python
Read personalities/dagster-expert/database/dagster-1.13.3/docs/data-version-and-staleness.md
```

**Semantic search** (when topic is fuzzy):
```
/search data version propagation in Style B with explicit deps
```

The v4 `/search` skill routes to SMAK against the active personality's
workspaces — confirm `dagster-expert` is active in `MEMORY.md` first.

Both work without switching active personality.

## Curator notes

- Pinned to Dagster 1.13.3. Upgrade → new `database/dagster-1.X.Y/`,
  don't mutate this dir.
- New gotchas get filed to
  `personalities/dagster-expert/memory/lessons_learned/_inbox/`
  first; curator promotes to a cheatsheet entry after review.
