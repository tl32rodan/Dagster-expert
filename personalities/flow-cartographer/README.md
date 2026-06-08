# flow-cartographer — Dagster orchestration framework workspace

This personality owns the design, spec, and verification for a
**Dagster-based orchestration framework** for the TSMC air-gap
environment (CentOS 7, LSF, tcsh, NFS, Dagster 1.13.x). It supports
**two execution modes** as first-class peers:

- **Mode A — sync-execution** (Dagster = lineage **and** execution).
  Reference: `spec_dagster/`. Best for ≤ hundreds of concurrent runs.
- **Mode B — async-execution / Execution Fabric** (Dagster = lineage
  only; execution outsourced). Reference: `execution_fabric/` (planned).
  Best for thousands+ of concurrent runs with long task durations.

**Pick a mode**: `ARCHITECTURE_CHOICE.md` has a 60-second decision tree.

## Where things live

| Artifact | Path | Role |
|---|---|---|
| **Mode chooser** ⭐ | `ARCHITECTURE_CHOICE.md` | The first thing to read. Decision tree (60s), adoption criteria, anti-patterns, migration paths between modes. |
| **Mode A architecture spec** | `FIVE_LAYER_WHITEPAPER.md` | Mode A's source of truth: 5 layers (M1 spec → M2 generator → M3 sensor → M4 launcher → M5 worker), every implementation contract, fourteen lessons (L1-L14) from the D1 build baked into the implementation-contract callouts, appendix C with the **5 behavioral equivalence aspects (C1–C5)** that every Mode-A application must pass. |
| **Mode A conversion plan** | `D2_IMPLEMENTATION_PLAN.md` | The plan for converting `liberate-char` (the first application) to run on the Mode-A framework. Five phases (C1–C5 spec → script → generated-dagster → equivalence test → EQUIVALENCE.md). **EXECUTED** in PR #18. |
| **Mode B architecture spec** | `EXECUTION_FABRIC_WHITEPAPER.md` | Mode B's source of truth: three-domain model (Dagster lineage / Execution Fabric / Control+UI), state machine with SUBMITTED/RUNNING split, three fault-handling contracts (dispatch dedup / orphan recovery / harvest idempotency), Phase 1 vs Phase 2 scope split. |
| **Mode B Phase 1 plan** | `PHASE_1_PLAN.md` | The plan for porting `liberate-char` from Mode A to Mode B Phase 1. v1→v2 layer map, file-level module breakdown (17 verbatim copies + 3 rewrites + 11 new modules), 1.13.3 API probe results (§10 Q4 ANSWERED 2026-06-05). **NOT YET BUILT**. |
| **Hand-rolled reference** | `examples/liberate-char/` | The pre-framework conversion of liberate-char (`converted/` is the runnable Dagster code; `flow-src/` is the original flow's source artifacts). The example the Mode-A framework was extracted from. |
| **Mode A framework + reference app** | `../spec_dagster/` (repo top-level) | The Mode-A implementation — `framework/`, `flows/liberate_char/`, `tests/`, `scripts/run_demo.py` + `scripts/equivalence.py`, `ONBOARDING.md`, `LESSONS.md` (L1-L14). Top-level so it can be packaged and moved independently. |
| **Mode B framework + reference app** | `../execution_fabric/` (planned, repo top-level) | The Mode-B implementation — same shape as `spec_dagster/` but with `fabric/` layer (status DB, file lock, LSF run client) and split sensor model (dispatch + harvest). |
| **Custom RunLauncher reference** | `../dagster-expert/learn/13-lsf-integration/` | Lesson 13 Part B in the dagster-expert corpus covers a **standalone reference** for a custom `LSFRunLauncher` — note this was a Mode-A scaling experiment retired in favor of Mode B's outsourced execution. Kept as decision-history artifact. |

## Quick navigation by intent

| You want to… | Read |
|---|---|
| **Pick which mode to use** | `ARCHITECTURE_CHOICE.md` (decision tree §3) |
| Understand Mode A (sync-execution) | `FIVE_LAYER_WHITEPAPER.md` |
| Understand Mode B (async-execution) | `EXECUTION_FABRIC_WHITEPAPER.md` |
| Compare the two modes head-to-head | `ARCHITECTURE_CHOICE.md` (§0 table, §1 essential distinction) |
| Implement a new flow on Mode A | `../spec_dagster/ONBOARDING.md` (and copy `../spec_dagster/flows/_template/`) |
| Implement a new flow on Mode B | `PHASE_1_PLAN.md` (build Mode B framework first, since `execution_fabric/` doesn't exist yet) |
| See a real, runnable Mode A application | `../spec_dagster/flows/liberate_char/` + `scripts/run_demo.py` |
| Verify Mode A equivalence vs hand-rolled | `../spec_dagster/scripts/equivalence.py`; `../spec_dagster/flows/liberate_char/EQUIVALENCE.md` |
| Diagnose a Dagster 1.13.3 quirk | `../spec_dagster/LESSONS.md` (L1–L14, also see `PHASE_1_PLAN.md` §10 Q4 for Mode-B-specific API probes) |
| Plan a Mode A flow-to-framework conversion | `D2_IMPLEMENTATION_PLAN.md` |
| Plan a Mode A → Mode B migration | `ARCHITECTURE_CHOICE.md` §6 + `PHASE_1_PLAN.md` §8 |
| Compare framework-generated vs hand-rolled | `examples/liberate-char/converted/` (the reference) vs `../spec_dagster/flows/liberate_char/` (the Mode-A framework version) |

## Current status (2026-06-05)

**Mode A (sync-execution)** — production-ready:

- **D1** built + verified: `spec_dagster` framework drives liberate-char
  (6 generators + 9 `characterize` leaves) end-to-end with real Dagster
  1.13.3 daemon + reconcile sensor. `0 → 3 → 7 → 9` partition wave
  (QueuedRunCoordinator `liberate_run: 4` cap), 23/23 runs succeeded,
  9/9 `.lib` + `.ldb` produced, determinism digest matches a direct
  reference run. Pure-function pytest 25/25 green; LSF launcher
  integration pytest 15/15 green (mock bsub/bjobs/bkill on PATH —
  no real cluster needed).
- **D2** equivalence: all five whitepaper-appendix-C aspects PASS
  behaviorally vs the hand-rolled `examples/liberate-char/converted/`
  (9/9 materialized, 9/9 `dagster/data_version` MATCH, 9/9
  path-free input chain MATCH, 9/9 `.ldb` digest MATCH, single-partition
  rerun isolated, instance migrate clean). Structural differences (the
  framework's reconcile sensor vs the hand-rolled `AutomationCondition.eager()`
  + drop sensor) are accepted designs, not behavioral.
- **Whitepaper edits absorbed from D1+D2**: L1 (annotations / context
  type), L2 (beta primitive), L3 (`resolve_asset_graph`), L4 (daemon
  absolute path), L6 (`default_status=RUNNING`), L9–L11 (event-records
  + data_version APIs), L13–L14 (test scaffolding — `@dg.job` for
  stand-ins; `register_instance` not `_instance=`).
- **Deferred** (out of D1 scope, called out in the whitepaper):
  M4 production `LSFRunLauncher` deployment (the code exists and is
  unit + mock-bsub-integration tested, but real-LSF + Postgres needs
  a cluster); the `work_items` dimensionality-reduction + batching
  sensor (the netlist_files reference flow path; liberate-char
  partitions `cell` directly).

**Mode B (async-execution / Execution Fabric)** — planned, not built:

- **Whitepaper landed** (`EXECUTION_FABRIC_WHITEPAPER.md`): three-domain
  model (Dagster lineage / Execution Fabric / Control+UI), state
  machine with SUBMITTED/RUNNING split, fault contracts.
- **Phase 1 plan ready** (`PHASE_1_PLAN.md`): file-level breakdown,
  17 verbatim copies from Mode A, 3 rewrites, 11 new modules,
  ≈ 8.5 days estimated. Key 1.13.3 API probe done (§10 Q4 ANSWERED):
  `report_runless_asset_event` + sensor-side event reporting both work,
  Option A path confirmed viable.
- **Implementation status**: awaiting decision on `PHASE_1_PLAN.md` §1
  (organizational: branch strategy, parallel `execution_fabric/` dir
  vs in-place, doc handling). Technical unknowns are cleared.
- **Scaling envelope**: Mode B targets >~thousands of concurrent runs
  with long task durations; below ~hundreds, Mode A is the better
  choice (less complexity for the same outcome). See
  `ARCHITECTURE_CHOICE.md` §3 decision tree.

## Why the personality is just three artifacts

Repo-root `AGENTS.md` describes this personality as
**whitepaper + D2 plan + examples**, intentionally minimal. The
hand-curated `personalities/flow-cartographer/AGENTS.md` was removed
during the 2026-06-04 personality cleanup; the lineage and Ripple
notes for that cleanup are in the whitepaper's appendix D. Everything
else (framework code, tests, scripts, onboarding doc) lives under
`../../spec_dagster/` (repo top-level) so it can evolve as code, with
its own pytest suite and demo harness, rather than being mixed into
the personality's instructional content.
