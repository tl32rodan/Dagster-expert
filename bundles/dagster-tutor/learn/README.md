# learn — Dagster progressive lessons

8 hands-on lessons. Work them in order; later lessons build on
earlier ones.

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

Total ≈ 10–12 hours of focused practice.

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
cd personalities/dagster-tutor/learn/<NN-topic>/
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
