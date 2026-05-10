# lab9 · real AP characterization flow

**Time**: 90–120 min · **Prerequisites**: lessons 01-08

## Why this lab exists

Lessons 01-08 introduced concepts in isolation. Lab 9 wires them
all into a **production-shaped** AP (Analog Performance)
characterization flow — the kind of pipeline TSMC's standard-cell
team actually runs. Plus three new ideas:

1. **Loose subprocess wrapping** — Perl scripts that don't know
   about Dagster (real-world: legacy Perl flows you can't rewrite)
2. **Pipes integration** — Python+TCL via `dagster_pipes` for the
   tightly-integrated steps (full event reporting, log forwarding)
3. **Fine-grain partition split** — step 4 (characterization) used
   to be one big TCL invocation per corner; now it's per-PVT, so
   one failed PVT only re-runs that one PVT
4. **Cross-partition fan-in** — step 2 (corner_setup) collects all
   24 LPE outputs of its corner via filesystem glob (Style B)
5. **Incremental rerun via checkpoint** — script-level `.done`
   marker so accidental re-materialize doesn't redo expensive char

## DAG

```
                        cell_list (Perl, root)
                              │
          ┌───────────────────┴───────────────────┐
          │      72 partitions: 3×24              │
          │      (corner) × (vt × cell)           │
          ▼                                       │
       lpe (Perl) ◄──────────────── fan-out per cell
          │
          │ fan-in (filesystem glob, by corner)
          ▼
   corner_setup (Python, per-corner) — 3 partitions
          │
          ▼
   netlist_gen (Python, per-corner) — 3 partitions
          │
          │ fan-out per (vt) — 18 partitions
          ▼
   liberate_run (Python+TCL via Pipes) — 18 partitions
          │       ⭐ THE CHARACTERIZATION STEP
          │       per-PVT checkpoint, FORCE_RERUN flag
          │
          │ fan-in (filesystem glob, by corner)
          ▼
   liberty_aggregate (Python, per-corner) — 3 partitions
          │
          │ fan-in across all corners
          ▼
   signoff_lib (Perl, leaf)
```

**Cardinality** (with the demo's small cell list):
- 4 cells × 6 PVT × 3 corners = **72 LPE partitions**
- 6 PVT × 3 corners = **18 char partitions**
- 3 corner-level partitions for setup / netlist / aggregate

In production: ~50 cells × ~12 PVT × 3 corners = ~1800 LPE
partitions, ~36 char partitions. Same shape, larger.

## File layout

```
09-real-flow/
├── README.md                      ← this file
├── workspace.yaml                 ← single-location workspace
├── _smoke.py                      ← end-to-end test driver
├── pipelines/
│   ├── __init__.py                ← re-exports defs
│   ├── partitions.py              ← partition definitions (3D-collapsed)
│   └── asset.py                   ← 7 assets + Definitions
└── scripts/
    ├── perl/
    │   ├── cell_list.pl           ← step 0
    │   ├── lpe.pl                 ← step 1
    │   └── signoff.pl             ← step 6
    ├── python/
    │   └── liberate_invoke.py     ← step 4 (Pipes-aware)
    └── tcl/
        └── char_one_pvt.tcl       ← step 4's actual EDA tool driver
```

## Run it

### Quick: end-to-end smoke

```bash
source ~/dagster-venv/bin/activate
cd ~/projects/personal-assistant/personalities/dagster-tutor/learn/09-real-flow
python -m _smoke
```

~30s. Materializes every partition through every asset; verifies
artifact counts at the end. Use this to confirm the lesson works
before exploring interactively.

### Interactive: dagster dev

```bash
cd ~/projects/personal-assistant/personalities/dagster-tutor/learn/09-real-flow
dagster dev -m pipelines
# open http://127.0.0.1:3000
```

In the UI:
1. **Catalog** → see the 7-asset graph with partition strips on
   `lpe` (72), `liberate_run` (18), corner-level assets (3 each)
2. Click `cell_list` → Materialize → green
3. Click `lpe` → "Materialize all" backfill → 72 partition runs
   (parallel, ~30s)
4. Repeat for downstream assets
5. Or skip the manual cascading: in the UI, select
   `liberty_aggregate` and click "Materialize and all upstream"

### Demo 1: per-PVT fine-grain rerun

Materialize one specific PVT only:

```bash
dagster asset materialize -m pipelines --select liberate_run \
    --partition 'corner=ff|volt_temp=0p9v__25c'
```

Or via Python API (matching the smoke test):

```python
from dagster import MultiPartitionKey, materialize
from pipelines.asset import liberate_run, defs

key = MultiPartitionKey({"corner": "ff", "volt_temp": "0p9v__25c"})
materialize([liberate_run], partition_key=key, resources=defs.resources)
```

This is the **fine-grain split** payoff: one PVT failed in production,
re-run only that PVT, not the 18-PVT batch.

### Demo 2: incremental rerun via checkpoint

Re-run the same partition you just materialized:

```bash
dagster asset materialize -m pipelines --select liberate_run \
    --partition 'corner=ff|volt_temp=0p9v__25c'
```

Look at the run logs — you'll see:

```
checkpoint hit at .../<corner>__<vt>.done; skipping char
(set FORCE_RERUN=1 to redo)
```

The script noticed the `.done` checkpoint, skipped the
expensive TCL invocation, and reported back a successful
materialization with the existing `.lib`'s data_version.

### Demo 3: force redo via env var

Set `FORCE_RERUN=1` in the shell, re-run:

```bash
FORCE_RERUN=1 dagster asset materialize -m pipelines \
    --select liberate_run \
    --partition 'corner=ff|volt_temp=0p9v__25c'
```

Now the run logs show:

```
invoking TCL: .../char_one_pvt.tcl
TCL stdout: char_one_pvt.tcl: ff/0p9v__25c -> .../ff__0p9v__25c.lib
```

The TCL was actually invoked and a fresh `.lib` was written.
This is the **escape hatch** when checkpoint logic gets stale
(e.g. you changed the TCL itself and want to redo) without
deleting checkpoints by hand.

## What's mocked vs real

The lesson code is structured exactly like a real flow; only the
underlying tools are mocked:

| Real | Mock |
|---|---|
| SOS query for cell list | hardcoded list in `cell_list.pl` |
| Cadence Quantus / Synopsys StarRC for LPE | tiny `.spef`-shaped text in `lpe.pl` |
| Cadence Liberate / Synopsys SiliconSmart | `tclsh char_one_pvt.tcl` writing tiny `.lib` |
| Liberty merging across PVTs | concatenation in `liberty_aggregate` |
| `tar` packaging for SOS shelf | text file in `signoff.pl` |
| ~30 min per char per PVT | ~150ms per char per PVT |

**Replacing the mocks with real tools** is a 1-line change per
script: swap the body. The Dagster integration around them
doesn't change.

## TSMC mapping notes

| Lesson asset | TSMC equivalent |
|---|---|
| `cell_list` | SOS-tracked cell directory enumeration |
| `lpe` | Quantus per (corner, V, T, cell) batch run |
| `corner_setup` | corner-level GDS / LEF / SDF preparation |
| `netlist_gen` | per-corner SPICE netlist + RC overlay |
| `liberate_run` | Liberate `-batch -execute` per PVT, the heavy step |
| `liberty_aggregate` | corner-level `.lib` merge for tools |
| `signoff_lib` | sign-off package: `tar` + manifest + push to SOS shelf |

The flow's data_version chain propagates upstream-content-driven
(Style B over filesystem). Each asset hashes its own output; if
upstream's output changed, downstream's input changes, downstream's
hash moves, and Dagster correctly sees downstream as stale.

## Pipes integration — what it gives you

`liberate_run` uses `PipesSubprocessClient` to invoke the Python
script that drives the TCL. The Python script imports
`dagster_pipes` and reports back via the official IPC channel:

- `pipes.log.info(...)` → events appear in Dagster's run log
  (you've seen "checkpoint hit" lines coming from inside Python)
- `pipes.report_asset_materialization(data_version=..., metadata=...)`
  → downstream Dagster knows the asset's data_version + metadata
  without the asset function needing to return `MaterializeResult`
  itself (the subprocess does it)

The Perl scripts (steps 0, 1, 6) DON'T use Pipes — they're plain
subprocesses. The asset function (Python) handles the
MaterializeResult after the subprocess exits. This is the right
pattern when:
- The subprocess is legacy code you can't modify
- The integration is "fire and check the output file" not
  "stream events back during the run"

For tightly-integrated steps where you want progress events
during a 30-min char, use Pipes (like `liberate_run`).

## Common gotchas

- **`dagster asset materialize --partition k1,k2,k3` doesn't work
  for MultiPartitions** — single-axis only. Use `dagster job
  backfill` for parallel multi-partition runs, or the Python
  `materialize()` API for in-process iteration (see `_smoke.py`).
- **MultiPartitionsDefinition 1.13.3 limit: 2 dimensions** — we
  collapse `vt + cell` into composite `vt_cell` keys for `lpe`.
  See `dagster-librarian/database/dagster-1.13.3/docs/partitions.md`.
- **Cross-partition fan-in via filesystem (Style B) requires**
  upstream output files to already exist when downstream runs.
  Dagster's job builder respects asset deps but doesn't
  pre-validate filesystem presence. Fail fast inside the
  downstream asset (see `corner_setup`'s explicit count check).
- **Checkpoint files persist across `dagster dev` restarts** —
  that's the point. Deleting `/tmp/dagster-09-flow/` resets
  everything; deleting just `liberate/.ff__0p9v__25c.done`
  resets one PVT.

## Cheat sheet

```python
# Multi-axis partition definition (1.13.3 max 2 axes)
from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition
pvt = MultiPartitionsDefinition({
    "corner": StaticPartitionsDefinition(["ff", "tt", "ss"]),
    "volt_temp": StaticPartitionsDefinition([...]),
})

# In an asset body, retrieve current partition keys
@asset(partitions_def=pvt)
def my_asset(context):
    keys = context.partition_key.keys_by_dimension
    corner, vt = keys["corner"], keys["volt_temp"]

# Pipes subprocess — full integration
@asset
def my_pipes_asset(context, pipes_subprocess_client: PipesSubprocessClient):
    return pipes_subprocess_client.run(
        command=["python3", "wrapper.py", "--arg", "value"],
        context=context,
        env={"EXTRA": "var"},
    ).get_materialize_result()

# Plain subprocess — loose integration
@asset
def my_perl_asset(context):
    subprocess.run(["perl", "script.pl", "--arg", "value"], check=True)
    output = Path("/tmp/output.file").read_bytes()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(output).hexdigest()[:16]),
        metadata={"path": "/tmp/output.file"},
    )

# Cross-partition fan-in via Style B filesystem
@asset(partitions_def=corner_partitions, deps=[AssetKey("upstream")])
def fan_in_asset(context):
    corner = context.partition_key
    upstream_outputs = sorted(Path("/tmp/upstream").glob(f"{corner}__*.spef"))
    # ... process all of them ...
```

## Now-try

1. **Force-fail one PVT, then re-run only it.** Edit
   `scripts/tcl/char_one_pvt.tcl`: add `if {$corner == "ff" && $vt == "0p9v__25c"} { exit 1 }`
   Reload, materialize liberate_run for ff/0p9v__25c → fails.
   Revert TCL, materialize the same partition → succeeds.
   Other 17 partitions are untouched.

2. **Add a 5th cell** to `cell_list.pl`'s hardcoded list
   (e.g. `OAI21`). Reload. Now `lpe`'s partition space grows to
   3×30=90, but old runs still count as fresh for the 72 they
   already covered. Materialize the new 18 partitions only.

3. **Rewire step 4 to invoke a different TCL script** (e.g.
   `char_with_em.tcl`). Verify `data_version` propagation:
   does `liberty_aggregate` go stale on the next run?
   (Spoiler: only if you re-materialize each affected PVT
   first — see the `data-version-and-staleness.md` cheatsheet.)

## Related cheatsheet entries

- `dagster-librarian/database/dagster-1.13.3/docs/partitions.md` —
  MultiPartitions 2D limit, partition selectors
- `dagster-librarian/database/dagster-1.13.3/docs/data-version-and-staleness.md` —
  why hash-of-output is the right pattern for filesystem-style assets
- `dagster-librarian/database/dagster-1.13.3/docs/style-a-vs-b.md` —
  why this lesson uses Style B for cross-partition fan-in
- `dagster-tutor/learn/06-interrupt-rerun/6d-checkpoint/` — the
  script-level checkpoint pattern this lesson generalizes
