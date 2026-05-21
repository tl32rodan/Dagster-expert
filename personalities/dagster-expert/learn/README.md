# learn — Dagster progressive lessons

20 hands-on lessons. Most readers should NOT do them all — see the
"Getting Started Path" below if you're an AP engineer new to
Dagster.

## Getting Started Path — for AP engineers (~3 hours)

Read these in order. You'll cover the core mechanics that make
the `demo/scale-lib/` reference implementation make sense.

| # | Lesson | Time | Why |
|---|---|---|---|
| 1 | [`01-asset-and-materialize`](01-asset-and-materialize/) | 30m | What's an `@asset`? What does "Materialize" do? |
| 2 | [`02-deps-and-lineage`](02-deps-and-lineage/) | 30m | How does Dagster know A depends on B? |
| 3 | [`03-partitions`](03-partitions/) | 45m | **The TSMC branch model lives here**: 46 branches = 46 partition keys on one asset. |
| 4 | [`12-scaling/`](12-scaling/) (just `compact/` subdir) | 60m | **The folder-as-asset pattern in miniature**: 1 asset per step + (lib×branch) as MultiPartitions. This is the architecture `demo/scale-lib/` uses at production scale. |
| Done | Read `demo/scale-lib/README.md` and `demo/scale-lib/WHAT_IS_REAL.md` | 30m | See the full pattern in context. Skip the rest until you need them. |

Optional add-ons (in priority order for AP work):

- [`06-interrupt-rerun`](06-interrupt-rerun/) (60m) — cancel + restart semantics; relevant for "I changed my mind, rerun this step".
- [`17-incremental-cross-partition`](17-incremental-cross-partition/) (60m) — when upstream changes ONE partition, only the matching downstream partitions go stale. This is the "incremental change event" promise of the two-tier model.
- [`18-cross-location-staleness`](18-cross-location-staleness/) (45m) — same as 17 but across code-location boundaries (multi-team / multi-library).
- [`19-auto-materialize-partitioned`](19-auto-materialize-partitioned/) (60m) — daemon does the per-partition rebuilds automatically; combines 17 + `AutoMaterializePolicy.eager()/lazy()`.
- [`20-multi-library-grain`](20-multi-library-grain/) (90–120m) — production-scale grain: 100 library × 21 step × 46 branch. group_name = library, branch = partition, full parent-mirror PartitionMapping. Bridge from `demo/scale-lib/`'s single-library reference to real-shop multi-library scope.

> 中文速查: 17/18/19 共用的 demo 路徑 + 坑點對照表見 [`INCREMENTAL_RERUN_GUIDE_zh.md`](INCREMENTAL_RERUN_GUIDE_zh.md). 公司端 demo 前過一次即可.

After this path, you can re-materialize specific (step, branch)
combos via the UI or `dagster asset materialize --select
lib_a/step1 --partition em` from the CLI. That replaces the
SOP-driven "rerun guess" with a deterministic operation.

## Full lesson catalog

Lessons below are **deep dives** — read on demand, not as a path:

| # | Topic | Time | What you take away |
|---|---|---|---|
| [01](01-asset-and-materialize/) | Asset & materialize | 30m | Smallest possible Dagster loop |
| [02](02-deps-and-lineage/) | Dependencies & lineage | 30m | What makes Dagster ≠ a job runner |
| [03](03-partitions/) | Partitions | 45m | The "for-loop" of Dagster |
| [04](04-runconfig/) | Run config | 30m | Parameterizing a run from the UI |
| [05](05-failures/) | Failures, retries, event log | 45m | Asset-level failure semantics |
| [06](06-interrupt-rerun/) | Cancel / kill / restart / checkpoint | 60m | Run-level state machine recovery |
| [07](07-cross-location/) | Cross-location dependencies | 30m | Multi-team / multi-library DAGs (incl. Day7 bug case) |
| [08](08-complex-deps/) | Complex dependency patterns | 90m | Sparse-matrix DAGs (route A vs B) |
| [09](09-real-flow/) | Real AP characterization flow | 90–120m | Production-shaped DAG: Perl + Python + TCL via Pipes; fine-grain split; incremental rerun via checkpoint |
| [10](10-branched-flow/) | Branched characterization (corner / lvf / em) | 60–90m | Multiple parallel char branches with different PVTRC sets; cross-branch deps (lvf/em → corner same-step); Style B fan-in |
| [11](11-multi-library/) | Multi-library + UI scaling | 60–90m | Programmatic asset generation per library (`key_prefix`); UI scaling via `group_name`; how 540-asset graphs stay navigable |
| [12](12-scaling/) | Scaling beyond SQLite | 90m | Diagnoses `SQLITE_MAX_VARIABLE_NUMBER` ceiling; compact MultiPartitions refactor (4500→15 assets); Postgres migration; per-library code locations for 100+ libs |
| [13](13-lsf-integration/) | LSF integration via Pipes | 60m | asset-body `bsub` pattern; `lsf_submit.py` wrapper; mock `bsub` for local dev; covers all 6 LSF requirements (log/cancel/state/env/parallel/throttle) |
| [14](14-schedules/) | Schedules — cron automation | 30m | `ScheduleDefinition` + cron expressions; AssetSelection-driven jobs; execution_timezone gotchas |
| [15](15-sensors/) | Sensors — event-driven automation | 45m | `@asset_sensor` / `@sensor` / `@run_status_sensor`; cursor for stateful polling; file-watch + run-finished patterns |
| [16](16-hooks-automaterialize/) | Hooks + auto-materialize | 45m | `@success_hook` / `@failure_hook` for callbacks; `AutoMaterializePolicy.eager()` vs `lazy()`; decision tree for which to use |
| [17](17-incremental-cross-partition/) | Cross-partition incremental rerun | 60m | Identity vs StaticPartitionMapping; data_version propagation across partitions; the constant-hash trap |
| [18](18-cross-location-staleness/) | Cross-location staleness propagation | 45m | Same incremental promise as 17 but across code-location boundaries; the multi-team boundary case |
| [19](19-auto-materialize-partitioned/) | AutoMaterializePolicy × partition | 60m | Daemon-driven per-partition rebuilds; eager() along critical path, lazy() on reports; what auto-materialize actually evaluates per tick |
| [20](20-multi-library-grain/) | Multi-library grain (100 lib × 21 step × 46 branch) | 90–120m | New AP-shop design: library=group, step=asset, branch=partition. Full parent-mirror StaticPartitionMapping. ~2.1k assets / ~47k partition records. Bridge target for real AP work (Tier-1 grain only; PVT/cell deferred to fine tier). |

Total ≈ 18–21 hours of focused practice.

## House rules

1. **Read the README first.** The lesson is in the prose; the code
   illustrates it. Reverse-engineering an asset.py without context
   wastes time.
2. **Do the "Now try" exercises.** They are the lab. Just running
   the sample code teaches you nothing past "Dagster runs".
3. **Pitfalls are at the bottom — read after the lesson works.**
   Most are version-specific footnotes; skim once and move on.

## Running any lesson

```bash
cd personalities/dagster-expert/learn/<NN-topic>/
# (or the sub-folder noted in that lesson's README)
dagster dev -m <module_name>
# open http://127.0.0.1:3000
```

Most lessons are single code locations and use `-m` directly.
Lesson 07+ uses multi-location workspaces with `-w workspace.yaml`.

## Prerequisites

- Python 3.10+ in a venv with `dagster==1.13.3` and
  `dagster-webserver==1.13.3` (and `dagster-pipes` from lesson 03+).
- A terminal you can leave running `dagster dev` while you click
  around in the UI.
- For air-gap installs: see the `dagster-operator` personality's
  `skills/bootstrap-airgap/SKILL.md`.

## When you get stuck

- **Concept not clicking?** Re-read the lesson README. Try the
  smallest variation of the code that breaks something. Failures
  teach faster than success.
- **Tooling broken?** Probably not a lesson issue — tell the
  agent "switch to dagster-operator" (no CLI command; the agent
  edits `MEMORY.md`'s active personality callout) and consult
  its diagnose-* skills.
- **Want to skip ahead?** OK, but lessons 02 and 06 are
  prerequisites for almost everything later. 03 unlocks 07 and
  08.
