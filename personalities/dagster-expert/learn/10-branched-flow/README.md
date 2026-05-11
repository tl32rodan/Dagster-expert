# lab10 · branched characterization (corner / lvf / em)

**Time**: 60–90 min · **Prerequisites**: lessons 02 (deps), 03
(partitions), 09 (real flow)

## Why this lab exists

In real TSMC AP characterization, "branch" doesn't mean "RC corner".
RC corners (ff/tt/ss) are values along the **PVTRC partition
axis**. The actual **branches** are orthogonal characterization
channels — each emits its own deliverable:

| Branch | What it characterizes | Typical PVTRC list |
|---|---|---|
| `corner` | Standard timing / power tables | Full sweep (ff/tt/ss) |
| `lvf` | Liberty Variation Format — statistical timing | Subset, often typical only (tt) |
| `em` | Electromigration constraints | Subset, typically the extremes (ff, ss) |

Each branch has its own subset of PVTRC points. Different
characterization techniques apply at different operating points.

## The dependency pattern Brian asked for

```
                cell_list (root, branch-agnostic)
                     │
       ┌─────────────┼─────────────┬──────────────┐
       │             │             │              │
       ▼             ▼             ▼              │
  corner.step1   lvf.step1     em.step1           │
       │             │             │              │
       │      (deps corner.1) (deps corner.1)     │
       │                                          │
       ▼             ▼             ▼              │
  corner.step2 ← lvf.step2 ←     em.step2         │
       ↑             ↑             ↑              │
       │      (deps corner.2  (deps corner.2      │
       │       + lvf.step1)    + em.step1)        │
       │                                          │
       ▼             ▼             ▼              │
  corner.step3 ← lvf.step3 ←     em.step3         │
                     ↑             ↑              │
              (deps corner.3 (deps corner.3       │
               + lvf.step2)   + em.step2)         │
                     │             │              │
                     └──────┬──────┘              │
                            │                     │
                            ▼                     │
                    cross_branch_signoff ◄────────┘
```

**Read the dep arrows carefully**:
- `corner.N` deps `corner.(N-1)` only — corner is self-contained
- `lvf.N` deps **`corner.N`** + `lvf.(N-1)` — needs same-step
  corner output and its own previous step
- `em.N` deps **`corner.N`** + `em.(N-1)` — same pattern

Critical: **same-step cross-branch dep**. `lvf.step2` needs
`corner.step2` (not `corner.step1`). `em.step3` needs
`corner.step3` (not lvf or anything else).

## Cardinality

| Branch | PVTRC list | Partitions per step | × 3 steps |
|---|---|---|---|
| corner | ff_125, tt_25, ss_m40 | 3 | **9** |
| lvf    | tt_25 (typical only) | 1 | **3** |
| em     | ff_125, ss_m40 (extremes) | 2 | **6** |
| total | | | **18** partition runs |

11 assets total: `cell_list` + 9 branch×step + `cross_branch_signoff`.

## Run it

### Smoke (one-shot)

```bash
source ~/dagster-venv/bin/activate
cd ~/projects/personal-assistant/personalities/dagster-expert/learn/10-multi-lib-cross-branch
python -m _smoke
```

~5s end-to-end. The driver enforces correct ordering:

```
step 0 → cell_list
step 1 → corner (all PVTRCs) → lvf (deps corner.1) → em (deps corner.1)
step 2 → corner (all PVTRCs) → lvf (deps corner.2) → em (deps corner.2)
step 3 → corner (all PVTRCs) → lvf (deps corner.3) → em (deps corner.3)
step 4 → cross_branch_signoff
```

If you violate this ordering (e.g. materialize `lvf_step1` before
`corner_step1`), the asset body raises with a clear message:
`"corner_step1 @ tt_25 missing at /tmp/.../corner/step1/tt_25.out;
materialize corner first"`. Style B's fail-fast pays off.

### Interactive (UI)

```bash
dagster dev -m pipelines
# open http://127.0.0.1:3000
```

In the lineage UI you'll see 11 nodes. Each branch's 3 steps
form a chain; the cross-branch arrows from `corner_stepN` →
`lvf_stepN`, `em_stepN` are visible as edges between the chains.

## Cross-branch dep — implementation note

We use **Style B** (filesystem fan-in), not `PartitionMapping`.
Reasons:

- Each branch has a different `partitions_def` object. Identity
  partition mapping won't work cleanly when partition sets
  differ (lvf has 1 key, corner has 3).
- `dagster.AllPartitionMapping` is overkill — we want
  *same-PVTRC* matching, not "fan in over all corner partitions".
- Style B is what real EDA flows do anyway: each tool reads files
  by known path. The Dagster lineage tracks the asset-level dep;
  the actual content propagation is filesystem-level.

The asset bodies look like:

```python
def lvf_step2_body(context):
    pvtrc = context.partition_key  # e.g. "tt_25"

    # Cross-branch dep: read corner_step2's output for THIS PVTRC
    corner_at_step = _read_corner_at(pvtrc, step=2)

    # Intra-branch dep: read lvf's own previous step output
    prev = _read_branch_prev("lvf", pvtrc, current_step=2)

    # Compute & write own output
    ...
```

The `_read_*` helpers raise loudly if the upstream file is
absent — preventing silent skip and giving the user a clear
"materialize X first" error.

## What this lesson does NOT cover

- **Multi-library** (svt / lvt / ulvt etc.) — would multiply the
  asset count by the library factor. The pattern (programmatic
  asset generation with `key_prefix=[<library>]`) is mentioned in
  the cheatsheet at `dagster-librarian/.../style-a-vs-b.md`. For
  this lesson we focus solely on the branch dep structure with
  one library implicit.
- **Tool wrapping** (Pipes, Perl) — covered in lesson 09.
  Lesson 10 keeps step bodies inline Python so the dep structure
  is in focus.
- **Custom `PartitionMapping`** — Style B sidesteps this. If you
  want partition-mapping-based same-PVTRC dep, see
  `dagster-librarian/.../partitions.md` (TODO) for the relevant
  classes.

## Now-try

1. **Force a missing-corner failure.** Materialize `lvf_step1` for
   `tt_25` BEFORE materializing `corner_step1` for `tt_25`. Expect
   the runtime error `"corner_step1 @ tt_25 missing at ..."`.

2. **Add a 4th PVTRC to `em`** — e.g. `tt_25` (typical-also-em).
   Edit `partitions.py`, add `"tt_25"` to `EM_PVTRCS`. Reload.
   Materialize the new partition. Verify `cross_branch_signoff`
   re-runs and includes the extra section.

3. **Rewire**: make `em.N` depend on `lvf.N` instead of
   `corner.N`. Edit `_make_secondary_step` in `asset.py`. Notice
   how the lineage graph changes shape; UI shows new arrow.

## Related cheatsheet entries

- `dagster-librarian/database/dagster-1.13.3/docs/style-a-vs-b.md`
  — Style B + filesystem fan-in for cross-partition shapes
- `dagster-librarian/database/dagster-1.13.3/docs/data-version-and-staleness.md`
  — propagation contract; remember `data_version` here is
  computed from each asset's own output (Style B convention)
- `dagster-tutor/learn/09-real-flow/` — single-pipeline shape
  (without the branch decomposition)
