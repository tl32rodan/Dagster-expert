# 17b · StaticPartitionMapping — different partition sets on each side

**Time**: ~25 min · **The production pattern.**

This is what `demo/scale-lib/` does to wire branch → branch
(e.g. `corner` partition of step-N → `{lvf, em, ht}` partitions
of step-(N+1)). When upstream and downstream have **different
partition definitions**, the default `IdentityPartitionMapping`
can't work — you must declare the wiring with
`StaticPartitionMapping`.

## Setup

```bash
cd 17b-static-mapping
dagster dev -m staticmap
# UI: http://127.0.0.1:3000
```

Two scenarios live in the same module:

| Scenario | Upstream | Downstream | Mapping |
|---|---|---|---|
| Fan-out (1 → N) | `root_branch` (1 partition) | `leaf_branches` (3 partitions) | `root → [lvf, em, ht]` |
| Routing (N → M) | `upstream_branches` (4) | `downstream_groups` (2) | `ff,sf → fast_group ; tt,ss → slow_group` |

## Scenario 1: fan-out (1 → N)

### Walkthrough

```bash
# materialize everything
dagster asset materialize -m staticmap --select root_branch --partition root
dagster asset materialize -m staticmap --select leaf_branches --partition lvf,em,ht
```

UI: all 4 partitions green.

Now bump `root_branch`'s payload (`rev=1` → `rev=2` in the
asset.py), reload, re-materialize `root_branch`. Open
`leaf_branches`: **all 3 leaf partitions are stale**.

That is the fan-out reality: changing the single root re-flags
every consumer. The mapping is what makes Dagster know "these
3 downstream partitions all depend on the same upstream
partition".

Re-materialize all 3 leaves (cheap; they each just re-hash the
upstream bytes). All green again.

## Scenario 2: routing (N → M, sparse)

### Walkthrough

```bash
dagster asset materialize -m staticmap --select upstream_branches --partition ff,tt,ss,sf
dagster asset materialize -m staticmap --select downstream_groups --partition fast_group,slow_group
```

UI: all 6 partitions green.

Now change ONE upstream partition's payload — say bump `rev=1`
to `rev=2` ONLY when `key == "ff"`. Easiest: edit
`upstream_branches`, change `payload = f"upstream__{key}__rev=1"`
to:

```python
payload = (
    f"upstream__{key}__rev=2"   # NEW for ff only
    if key == "ff"
    else f"upstream__{key}__rev=1"
).encode()
```

Reload code locations, re-materialize ONLY the `ff` partition of
`upstream_branches`.

**Now look at `downstream_groups`**:

- `fast_group` — **stale** (consumes `ff` + `sf`; `ff` changed)
- `slow_group` — **fresh** (consumes only `tt` + `ss`)

Re-materialize `fast_group` only. The asset's function arg
`upstream_branches: dict[str, bytes]` is a dict containing both
`ff` and `sf`'s current bytes — Dagster fans them in via the
`StaticPartitionMapping`. Logs in the run will show
`upstream_count=2`, `upstream_keys=ff,sf`.

That's the production case: sparse routing means changes in one
upstream branch only flag the matching downstream group — not
every downstream.

## Why the function signature differs

In 17a (identity mapping, 1:1), the downstream's arg was a
single `bytes` value. Here in 17b's scenario 2 the downstream
consumes *multiple* upstream partitions per its own partition, so
the arg is a `dict[str, bytes]` keyed by upstream partition key.
Dagster's default IOManager does this fan-in for you when the
mapping is many → one.

(Scenario 1 — fan-out — is the opposite shape: many downstream
partitions each consume the *same* one upstream partition. So
the arg is a single `bytes`, not a dict.)

## Common gotchas

- **`mapping target partitions not in the downstream partitions
  definition`** — your `StaticPartitionMapping` declares a target
  key the downstream's `partitions_def` doesn't have. This is the
  exact bug PR #7 fixed in `demo/scale-lib/` (translator was
  passing 46 branch keys for an asset that only had 1).
  Fix: filter the mapping values to keys present in the
  downstream's `partitions_def`.
- **`upstream_branches` arg is a single `bytes`, not a `dict`** —
  you forgot the `partition_mapping=` in `AssetIn`, so Dagster
  inferred Identity and tried to match a `ff` upstream to a
  `ff` downstream that doesn't exist. The "upstream" then becomes
  missing and the run fails at load.
- **`StaticPartitionMapping` looks like dead code in production
  scale-lib** — it isn't; the translator writes one per dep edge.
  The cheatsheet entry calls out that custom subclasses of
  `PartitionMapping` are deprecated in 1.13.3 — stick with
  the built-in `StaticPartitionMapping` / `IdentityPartitionMapping`
  / `AllPartitionMapping`.

## What to try next

→ **17c** — what if your downstream's hash is computed from a
constant rather than upstream bytes? Staleness silently breaks.
This is the most common production bug for `data_version`
chains. The cheatsheet calls it "the constant-hash trap"; 17c
makes you watch it happen.
