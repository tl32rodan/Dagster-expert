# mock-char-dagster — scaffold a char_dagster-shaped Dagster 1.13.3 reference project from a real flow

Given a real characterization flow (its source tree + a CharConfig-like YAML), this skill produces a `char_dagster/`-shaped reference project that a **less-capable internal agent** (Minimax M2.5 / Kimi K2.5 on a TSMC air-gap workstation) can then adapt asset-by-asset to convert the real flow to Dagster. The canonical fork point is `personalities/flow-cartographer/examples/char_dagster/`; this skill scaffolds a new one with shape matched to the new flow.

## When to invoke

Triggers: "generate a char_dagster for X", "scaffold a mock Dagster project from this flow", "I need a reference project the internal agent can copy from", "build a Liberate-style Dagster demo from this spec", "char_dagster from `<flow>`".

If the user only says "generate a char_dagster" without naming the flow, ask **what the new flow's `trio_groups`, `pvts`, and `cells` are** before doing anything else.

## Core goal (one sentence — laddered to every decision below)

> Hand the internal weak agent a **shape that is already correct** so its job becomes filling in templates, not designing.

"Shape correct" means partition vocabulary, asset roster, dep graph, sensor flow, and partition mappings are pinned BEFORE the internal agent sees the project.

## Three pillars

| Pillar | What it solves | How it shows up |
|---|---|---|
| **① Shape alignment** | weak agent can't design from scratch, only copy | start from `examples/char_dagster/`; only change what truly differs (trio_group vocabulary, pvt list, cell list, lpe_rc derivation, template content) |
| **② Designed for a weak agent** | judgment-style rules get skipped | every rule a mechanical trigger ("before writing `from dagster import X`, run `/search X`; 0 hits ⇒ REFUSE"), borrowed from `MEMORY.md` eight lessons |
| **③ Correctness guardrails** | weak agent walks into invisible Dagster reconciliation traps | built-in partition mappings only; never subclass `PartitionMapping`; `StrictUndefined` on Jinja; module-level singletons; one bsub per partition; folder-as-asset = one `MaterializeResult` per partition |

## Pre-flight (tick out loud before any code)

All paths below are **placeholders the operator fills in** — do not assume any specific filesystem layout. The skill itself bakes in no absolute paths.

```
[ ] 1. echo $DAGSTER_HOME -> non-empty (tcsh: setenv DAGSTER_HOME <path>; bash: export DAGSTER_HOME=<path>)
[ ] 2. which dagster && dagster --version (expect 1.13.3)
[ ] 3. python -c "import jinja2" (air-gap check)
[ ] 4. cat <flow-config-yaml> -> shows trio_groups + pvt_corners + cells
[ ] 5. ls <flow-templates-dir>/ -> every source template enumerable
[ ] 6. LIBRARIAN-check planned imports against <repo-root>/personalities/dagster-expert/database/dagster-1.13.3/store/
[ ] 7. tcsh-first; absolute paths only — no `cd` chains
[ ] 8. AIR-GAP: local `git commit` only; do NOT `git push`; do NOT create a PR
```

Any box fails → stop and report. Do not proceed.

## Workflow

### Step 1 — Confirm the four shape questions

Before any code, get answers (ask the user; do not invent defaults):

1. **`derive_lpe_rc` rule** — how is `lpe_rc` derived from `trio_group`? Default in the canonical fork: drop the process-variant token at index 1 after splitting on `_`, e.g. `LPE_ssgnp_cworst_T_25c → LPE_cworst_T_25c`. Confirm or override.
2. **Model_card per-trio variation** — does Model_card content truly differ across trio_groups (2D `trio_group × pvt`), or is it pvt-only (1D `pvt`)? Wrong default forces a needless 2D partition.
3. **`main.tcl` partitioning** — one unpartitioned main.tcl that takes trio_group + pvt at runtime, or one per (trio_group, pvt)?
4. **Project location** — where to write (default: `personalities/flow-cartographer/examples/<flow_name>_dagster/`).

### Step 2 — Cardinality math FIRST

Enumerate before fixing shape. Per `MEMORY.md` "Cardinality math first when scaling": the math, not the framework's API surface, drives whether 2D or 3D, whether per-cell or per-pvt fan-out.

```
trio_groups × pvts × cells = <N>
characterize partitions    = trio_groups × pvts
netlist partitions         = trio_groups × cells
```

**Hard cap: 2D** (`trio_group × pvt` or `trio_group × cell`). Never 3D — restructure cell expansion into the asset body instead.

### Step 3 — Fork and adjust

Start from the canonical fork point — `personalities/flow-cartographer/examples/char_dagster/` relative to the Dagster-expert repo root (the operator supplies the absolute repo location):

```tcsh
cp -r <canonical-fork-point> <target-dir>
```

Adjust **only**:

- `config/char_config.yaml` — replace trio_groups, pvts, cells, project.name with the real flow's values
- `char_dagster/paths.py` — adjust `derive_lpe_rc` if the real rule differs from default; adjust path constructors if directory layout differs
- `templates/*.j2` — replace each body to match the real flow's TCL/SPI conventions; **preserve placeholder names** so asset code does not need to change
- `bin/liberate` — adjust mock to match the real liberate's input file naming and output `.lib`/`.ldb` layout
- `README.md` — update tree / examples for the new flow

Leave as-is unless a clear reason emerges:
- `char_dagster/utils.py`, `config.py` (schema), `partitions.py`, `spec/mappings.py`, `sensor.py`, `definitions.py`, `assets/{source_generation,execution}.py`, `lsf_inner.py`
- `bin/bsub`
- the asset roster, dep graph, and partition mappings table
- the 8-box pre-flight in README

### Step 4 — Smoke test (no Dagster import needed)

```tcsh
cd <target-dir>
python tests/_smoke.py
```

Must pass: emits N `.lib` + N `.ldb` where N = `trio_groups × pvts × cells`. Zero leftover Jinja placeholders. If not, stop and diagnose; do not commit.

### Step 5 — Local commit (NOT push)

The air-gap workstation has no remote and no internet. ONLY `git commit`:

```tcsh
cd <repo-root>
git add <target-dir>
git commit -m "feat: scaffold <flow_name>_dagster from char_dagster template"
```

Do NOT run `git push`. Do NOT open a PR. The user syncs outwards via their own transport-safe channel.

## Hard rules (mechanical refusals — same as `examples/char_dagster/`)

| Trigger | Required action | If skipped |
|---|---|---|
| About to write `from dagster import X` | LIBRARIAN-check via `/search X`; 0 hits ⇒ REFUSE | Inventing nonexistent APIs leaks to the air-gap box |
| About to write `class Foo(PartitionMapping)` | REFUSE — use built-ins only (`Identity`, `All`, `MultiToSingleDimension`, `MultiPartitionMapping`, `Static`) | Breaks reconciliation / auto-materialize |
| About to write `class Foo(RunLauncher)` | REFUSE — STANDARD_USAGE §8/§9c | Wrong layer; LSF concurrency goes via `QueuedRunCoordinator` + `dagster/concurrency_key` |
| About to add a 3rd dim to `MultiPartitionsDefinition` | REFUSE — refactor cell expansion into asset body | Readability collapse |
| About to use `string.Template` for TCL/SPI | REFUSE — Jinja2 only | TCL `${var}` collides |
| About to set Jinja env without `StrictUndefined` | REFUSE | Silent half-rendered files reach `liberate` |
| About to `subprocess.run(["bsub", ...])` directly from an asset | REFUSE — wrap via `PipesSubprocessClient` (or the project's LSF launcher abstraction if one exists) | Loses Pipes events |
| About to run `git push` or open a PR | REFUSE — air-gap, local commit only | No remote / no internet |
| About to launch `dagster dev` | Verify `echo $DAGSTER_HOME` non-empty; else REFUSE with `setenv DAGSTER_HOME …` | Default `~/.dagster` pollutes lessons |
| About to use a relative path in a `dagster` CLI invocation | REFUSE — use absolute `-w /abs/path/workspace.yaml` | `cd` chains break across turns |
| About to write `setenv` for bash users | Show `setenv …` first, then `export …=…` in parentheses | User is tcsh per `MEMORY.md` |

## Reference materials

| Need | Path |
|---|---|
| Canonical fork point | `personalities/flow-cartographer/examples/char_dagster/` |
| Dagster 1.13.3 API corpus (LIBRARIAN) | `personalities/dagster-expert/database/dagster-1.13.3/store/` |
| Standard usage rules (§3.2 partition mappings, §8 LSF, §9 folder-as-asset) | `personalities/dagster-expert/database/dagster-1.13.3/docs/STANDARD_USAGE.md` |
| Gotchas (esp. #4 never-subclass-PartitionMapping) | `personalities/dagster-expert/memory/understanding/dagster-1.13.3-gotchas.md` |
| 2D↔1D mapping live example | `personalities/flow-cartographer/examples/liberate-char/converted/pipelines/deps.py` |
| `AutomationCondition.eager()` live usage | `personalities/flow-cartographer/examples/liberate-char/converted/pipelines/assets.py:97` |
| 8 design lessons for weak agents | `MEMORY.md` "Lessons learned" section |

## Out of scope

- Subclassing `RunLauncher` (Dagster's built-in interface) — forbidden per STANDARD_USAGE §8/§9c. A **Python helper module** for LSF submission is in scope (see the project's `char_dagster/lsf_inner.py` and any project-level LSF launcher abstraction).
- 3D `MultiPartitionsDefinition` — 2D cap.
- `string.Template` substitution — TCL `${var}` collides.
- `git push` / PR creation — air-gap.
- Diff-proof against a "before" tree — this skill scaffolds new; it does not audit existing.
