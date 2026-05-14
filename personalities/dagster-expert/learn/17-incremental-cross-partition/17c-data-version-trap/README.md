# 17c · constant-hash trap — staleness propagation silently breaks

**Time**: ~15 min · **The most common production bug for
data_version chains.**

The cheatsheet calls this "the constant-hash trap"
(`personalities/dagster-expert/database/dagster-1.13.3/docs/data-version-and-staleness.md`).
This sub-lab makes you watch it break.

## Setup

```bash
cd 17c-data-version-trap
dagster dev -m trap
# UI: http://127.0.0.1:3000
```

Three assets in a line, same partition def:

```
raw_corner  ─►  mid_corner  ─►  final_corner
   ✓             ✗ broken           ✓
```

`raw_corner` and `final_corner` are correct (Style B — hash own
output bytes). `mid_corner` is **broken on purpose**: its
data_version is `hash("mid_constant_for__<partition_key>")`,
which never changes across re-materializations of the same
partition.

## Walkthrough

### Step 1 · materialize the chain

Backfill all 4 partitions on all 3 assets. All green.

```bash
dagster asset materialize -m trap --select raw_corner --partition ff_125c,tt_25c,ss_m40c,ss_125c
dagster asset materialize -m trap --select mid_corner --partition ff_125c,tt_25c,ss_m40c,ss_125c
dagster asset materialize -m trap --select final_corner --partition ff_125c,tt_25c,ss_m40c,ss_125c
```

### Step 2 · bump upstream

Edit `trap/asset.py` in `raw_corner`:

```python
payload = f"raw__{key}__rev=1".encode()   # change rev=1 to rev=2
```

Reload code locations.

### Step 3 · re-materialize one partition of `raw_corner`

```bash
dagster asset materialize -m trap --select raw_corner --partition ff_125c
```

UI: open `mid_corner` partitions. `ff_125c` is **stale** — good,
Dagster noticed. (The other 3 are fresh; identity mapping
works.)

### Step 4 · re-materialize `mid_corner`'s `ff_125c` partition

```bash
dagster asset materialize -m trap --select mid_corner --partition ff_125c
```

`mid_corner` `ff_125c` is now "fresh" again. **Now look at
`final_corner`**: `ff_125c` is STILL FRESH. UI never flagged
it stale.

### Step 5 · prove the breakage

UI → `mid_corner` → Materializations tab → `ff_125c` row.

You'll see **two materialization events** for `ff_125c`:

| Materialization | Data version |
|---|---|
| Before edit | (constant hash for ff_125c) |
| After edit  | (same constant hash for ff_125c) |

**The Data version column is identical across the two
materializations.** That's the bug. Dagster compares this
column to decide downstream staleness; identical = downstream
not stale.

`final_corner` therefore has no reason to re-run. The file
`mid_<ff_125c>.bin` it would read from disk hasn't changed
either (since `mid_corner`'s "work" is also a constant write).
So even if you manually re-materialize `final_corner`, its
content is the same as before — the user's edit at `raw_corner`
never reached `final_corner`.

## The fix

`mid_corner` must compute its data_version from something that
actually changes when upstream changes. Two correct shapes:

### Style A (function arg + IOManager)

```python
@asset(partitions_def=corner_partitions)
def mid_corner(raw_corner: bytes) -> MaterializeResult:
    output = b"mid_of:" + raw_corner
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
    )
```

### Style B (deps + read upstream's file)

```python
@asset(partitions_def=corner_partitions, deps=[AssetKey("raw_corner")])
def mid_corner(context):
    key = context.partition_key
    upstream_bytes = (OUT_DIR / f"raw_{key}.bin").read_bytes()
    output = b"mid_of:" + upstream_bytes
    (OUT_DIR / f"mid_{key}.bin").write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
    )
```

(This is exactly what 17a does.)

Both correctly fold upstream content into the downstream's hash,
so the version actually moves.

## What NOT to "fix" it with

The cheatsheet has a worked example of the *wrong* fix — reading
upstream's stored `data_version` from `context.instance` via a
`dagster._core.*` private import and folding that into your
hash. Don't. Reasons:

1. `_core.*` is a private path → breaks across Dagster minor
   versions.
2. You're using Dagster metadata as a proxy for content. If
   upstream itself has the constant-hash bug, you inherit it.

If you're tempted to do this, you actually wanted Style A or
Style B.

## Diagnosing this in production

Symptom: edit upstream, re-materialize, downstream-of-downstream
stays fresh. Procedure:

1. UI → middle asset's Materializations tab → does
   `Data version` actually change between two consecutive
   materializations of the same partition?
2. If NO → middle asset has the constant-hash bug. Fix with
   Style A or Style B.
3. If YES → propagation works; the downstream-of-downstream
   just hasn't been re-materialized yet (you only re-ran the
   middle).

## Why this matters for AP

The "incremental rerun" benefit only works if the *whole chain*
correctly propagates `data_version`. One bad middle asset breaks
it for everything downstream. In a 21-step library where step 7
is broken, re-running step 1 will silently fail to flag steps
8-21 as stale — you'd backfill all of them by hand, defeating
the point.

Audit hint: in CI, validate that for each pair of consecutive
assets, the downstream's data_version actually changes when
upstream content changes. The `dagster-ap-auditor` personality
(in this repo) has skills for this kind of acceptance check.

## What to try next

You've now seen all three flavors of cross-partition incremental
behavior. Read `learn/17-incremental-cross-partition/README.md`
for the summary table and where each pattern lives in
`demo/scale-lib/`.
