# scale-lib — what's real, what's mock

A field guide for anyone (human or agent) about to swap demo
placeholders for production reality. **Read this before
modifying anything in `scripts/` or `config/`.**

## The one-line spec of this demo

> **One step = one folder. Folder content → SHA256 digest →
> data_version. Two reruns with same content → same digest →
> Dagster knows no propagation needed.**

Everything else is scaffolding around this. The scaffolding is
either (a) production-grade design that survives into real use,
or (b) MOCK placeholders that exist so the demo can run without
real EDA tools / TSMC infrastructure.

## Inventory

### ✅ Real production-grade design (KEEP — these survive into real use)

| Component | File(s) | Why it's real |
|---|---|---|
| **Folder-as-asset contract** | `CONTRACT.md`, `pipelines/folder_digest.py` | The whole point. Algorithm + invariants apply to real flows unchanged. |
| **Tier 1 shape**: library × step, branch as partition | `pipelines/factory.py`, `pipelines/partitions.py` | Matches how AP is operationally organized today. |
| **4-layer dep architecture** (spec / rules / registry / translator / factory) | `pipelines/spec/`, `pipelines/rules/`, `registry.py`, `translator.py`, `factory.py` | Keeps dep facts in one place; layer-import test enforces. Reviewable at scale. |
| **Layer-import enforcement** | `tests/test_layer_imports.py` | Greps source for forbidden `import dagster` in Layer 0-2. Production-essential discipline. |
| **Variant tree** (46 branches with parent relations) | `config/branches.yaml`, `pipelines/spec/branch_hierarchy.py` | Mirrors TSMC's actual variant tree structure (corner → em / ht / lvf / lvf_ht / mpwda / tmsf_self → ...). |
| **Step taxonomy** (21 step types with kinds) | `pipelines/spec/step_taxonomy.py` | Mirrors AP's actual step set (step0..step7 + pgv/apl/cdk/mtbf/trf/spm/rln/... ). |
| **`subprocess.run([...])` runner** | `pipelines/runners.py` | Real wrapper invocation pattern. Swap point for LSF (`["bsub", "-K", ...]`). |
| **89-test pytest suite** | `tests/` | Reusable for prod; layer-import + integration patterns transfer. |

### 📋 Real TSMC-specific data (KEEP — these are not placeholders)

| Component | Source |
|---|---|
| `config/branches.yaml` 's 46 branches + parent relations | Brian's actual AP variant tree |
| `pipelines/spec/step_taxonomy.py` 's 21 step kinds | Brian's actual AP step set |

### 🟡 MOCK content (replace at office, one item at a time)

| Component | Current state | Replace with |
|---|---|---|
| `scripts/perl/*.pl` bodies | All symlink to `_template.pl` — sleep + write fake content | Real Perl flow scripts. Same I/O contract: read CLI args, write into `$ARGS{--out}`, exit 0. |
| `scripts/python/*.py` bodies | Same — symlink to `_template.py` | Real Python flow scripts. |
| `lib_a` library name | Hard-coded in `definitions.py` | Real library names: `svt`, `lvt`, `lvtll`, `ulvt`, `ulvtll`, `elvt`. Add per-library `Definitions` in factory, or split into per-library code locations (Tier-1 scaling Level 3). |
| `config/pvt_manifest.yaml` | Mock empty / placeholder | Real PVT spec export from TSMC AP (when that export point exists). |
| `config/cells.json` | Mock cell list | Real cell list from SOS library or wherever. |
| `pipelines/source_observers.py` `pvt_manifest_source` | Hashes a mock file; **not currently a dep of any step** | When real PVT spec is exported: keep the observer; register `PvtSourceRule()` in `_default_rules()` to wire it as upstream of `apl`/`pgv`/`cdk`. |
| `pipelines/source_observers.py` `cell_list_source` | Same as above, no real wiring | Wire similarly when real cell list source is identified. |

### 🟠 Not yet wired (deferred — features exist as templates / disabled)

| Component | Why deferred | Activation cost |
|---|---|---|
| Multi-library (only `lib_a`) | Tier-1 single-library is enough for v1 demo; multi-library is Phase 3 of adoption | ~1 day: replicate factory call per library in `definitions.py`, decide partition strategy (Level 2 single-location vs Level 3 per-library code locations) |
| LSF integration | `runners.py` is the swap point but `subprocess.run` defaults to local | ~1-2 days: rewrite runner to bsub; ensure shared FS for `.dagster_meta.json`; verify `bsub -K` cancel propagation |
| PVT-source incremental ("Item 1") | Source asset exists but `PvtSourceRule` is intentionally NOT in `_default_rules()` because no real PVT export point | <1 hour after real export point is wired |
| Tier 2 framework | Demo runs entire step as one subprocess; per-PVT / per-cell fan-out happens INSIDE the script body | Significant — see `memory/understanding/why-two-tier.md` § Phase 3 |
| Cross-branch deps (variant-tree mirror) | Implemented in `rules/parent_mirror.py` + `step_chain.py`, **activated** for step5 in `_default_rules()` | Already real. To extend: edit `ParentMirrorRule(applies_to=frozenset({"step5", "step3", ...}))` |
| AP-compat touch file (`.ap_done`) | Mentioned in `CONTRACT.md`; not implemented | <1 hour: 1-line in step scripts to touch the marker after exit |

### 🔴 Hypothetical (not promised, not in this PR)

| Idea | Why not built |
|---|---|
| Observer-mode Tier 1 | Brian's adoption path is "active take-over from day 1", not gradual observation. If that changes, see `memory/understanding/why-two-tier.md` § Phase 1. |
| Auto-mat policies on step assets | Step materialization is currently manual / sensor-driven; auto-mat would re-trigger steps on every observation tick. Defer. |
| Manual-override sentinel (`.dagster_no_rerun`) | Hypothetical UX case; not promised. |

## Decision flow when something doesn't work as expected

Use this when an agent says "I'll add X to fix Y":

1. **Is Y a real production painpoint** (one of #1–#3 in `memory/understanding/why-two-tier.md`)? If no — push back.
2. **Is X in the "Real production-grade design" table above?** If yes — proceed. If X is in MOCK, ensure it's marked MOCK in code/comments.
3. **Does X introduce mock content into the dep registry / production code path** (not test/mock dirs)? If yes — refuse. MOCK belongs in `config/` or `scripts/` only.
4. **If X is real design but the dependency it needs is mock** (e.g. `pvt_manifest` → would activate `PvtSourceRule`): file as deferred work in this doc's "Not yet wired" table. Do NOT activate the rule in `_default_rules()` until the mock dependency is replaced.

## Quick "is the demo healthy?" check

Run `./RUN_DEMO.sh` from this directory. It verifies:
- Python venv + Dagster 1.13.3 present
- `dagster definitions validate -w workspace.yaml` PASS
- `pytest tests/` PASS (81 unit + integration)
- `python -m _smoke` PASS (end-to-end 16 materializations)
- `dagster dev` launches; UI reachable; GraphQL returns expected
  asset count + lineage

If any step fails, the script prints which one and exits
non-zero. Re-runnable.

## Mapping mock → real (cheat sheet for office migration)

| To replace this MOCK | Edit this file | Then run |
|---|---|---|
| One step's Perl body | `scripts/perl/<step>.pl` (currently a symlink to `_template.pl`) — replace with real script | `python -m pytest tests/` + `python -m _smoke` |
| One step's Python body | `scripts/python/<step>.py` | Same |
| `lib_a` placeholder | `pipelines/definitions.py` — change library name; or add more factory calls per library | Validate + smoke |
| `pvt_manifest.yaml` | `config/pvt_manifest.yaml` — write real PVT spec; then uncomment `PvtSourceRule()` in `pipelines/registry.py::_default_rules` | Validate + smoke |
| LSF on/off | `pipelines/runners.py` — swap `subprocess.run([perl, ...])` for `subprocess.run(["bsub", "-K", ...])` | Validate + smoke |

## References

- `CONTRACT.md` — Tier-1 / Tier-2 boundary spec (what step scripts must honor)
- `memory/understanding/why-two-tier.md` — the painpoint motivation (in PR #8)
- `learn/12-scaling/` — scaling levels (in PR #6, lesson 12 calibration)
- `learn/13-lsf-integration/` — LSF mock + real swap recipe
