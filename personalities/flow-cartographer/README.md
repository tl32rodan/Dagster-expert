# flow-cartographer — five-layer Dagster framework workspace

This personality owns the design, spec, and verification for a
**spec-driven five-layer Dagster framework** that lets a flow owner
declare an asset graph in YAML and gets a runnable Dagster code
location for free (assets, partitions, partition-mappings, jobs, and a
reconciliation sensor — all generated from the spec). It targets the
TSMC air-gap environment: CentOS 7, LSF, tcsh, NFS, Dagster 1.13.x.

## Where things live

| Artifact | Path | Role |
|---|---|---|
| **Architecture spec** | `FIVE_LAYER_WHITEPAPER.md` | The single source of truth for what the framework MUST be: 5 layers (M1 spec → M2 generator → M3 sensor → M4 launcher → M5 worker), every implementation contract, ten lessons from the D1 build baked into the implementation-contract callouts, appendix C with the **5 behavioral equivalence aspects (C1–C5)** that every application built on the framework must pass. |
| **D2 plan** | `D2_IMPLEMENTATION_PLAN.md` | The plan for converting `liberate-char` (the first application) to run on the framework. Five phases (C1–C5 spec → script → generated-dagster → equivalence test → EQUIVALENCE.md). |
| **Hand-rolled reference** | `examples/liberate-char/` | The pre-framework conversion of liberate-char (`converted/` is the runnable Dagster code; `flow-src/` is the original flow's source artifacts). Used as the **equivalence target**: the framework must produce behaviorally-identical Dagster from a spec.yaml that this example expresses in hand-written Python. |
| **Framework + reference app** | `../spec_dagster/` (repo top-level) | The actual implementation — `framework/` is the framework, `flows/liberate_char/` is liberate-char expressed as `spec.yaml + script.py`, `tests/` is the pure + integration pytest suite, `scripts/run_demo.py` and `scripts/equivalence.py` are the two end-to-end verifications, `ONBOARDING.md` is the flow-owner SOP, `LESSONS.md` records what the build taught us about 1.13.3 / the whitepaper. It lives at repo top-level (not under this personality) because it is framework code, not personality content — and so it can be packaged and moved independently. |
| **Custom RunLauncher reference** | `../dagster-expert/learn/13-lsf-integration/` | Lesson 13 Part B in the dagster-expert corpus covers the **standalone reference** for a custom `LSFRunLauncher` (the M4 large-scale path). The framework's actual launcher lives at `../spec_dagster/framework/launcher/lsf_run_launcher.py`. |

## Quick navigation by intent

| You want to… | Read |
|---|---|
| Understand the architecture (5 layers, sensor model, launcher contract, Postgres backend) | `FIVE_LAYER_WHITEPAPER.md` |
| Implement a new flow on the framework | `../spec_dagster/ONBOARDING.md` (and copy `../spec_dagster/flows/_template/`) |
| See a real, runnable application | `../spec_dagster/flows/liberate_char/` + `scripts/run_demo.py` |
| Verify a flow's behavioral equivalence vs a hand-rolled reference | `../spec_dagster/scripts/equivalence.py` (template); `../spec_dagster/flows/liberate_char/EQUIVALENCE.md` (a filled-out example) |
| Diagnose a Dagster 1.13.3 quirk hit during the build | `../spec_dagster/LESSONS.md` (L1–L14, each cross-referenced to the whitepaper sections that absorbed it) |
| Plan a flow-to-framework conversion | `D2_IMPLEMENTATION_PLAN.md` |
| Compare framework-generated vs hand-rolled | `examples/liberate-char/converted/` (the reference) vs `../spec_dagster/flows/liberate_char/` (the framework version) |

## Current status (2026-06-05)

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
