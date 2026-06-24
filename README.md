# Dagster-expert

An execution framework for large-scale EDA characterization on an
air-gapped TSMC workstation, layered on **Dagster 1.13.7** + **LSF**.

## What lives here

| What | Where | For |
|---|---|---|
| Architecture white paper (strategy + design rationale) | [`WHITEPAPER.md`](WHITEPAPER.md) | engineers + agents |
| Framework code + worked example (`liberate_char`) | [`execution_fabric/`](execution_fabric/) | engineers |
| Dagster 1.13.7 air-gap corpus + the one librarian skill | [`personalities/dagster-expert/`](personalities/dagster-expert/) | agents |
| Migration coach personality (porting your existing pipeline) | [`personalities/flow-cartographer/`](personalities/flow-cartographer/) | engineers + agents |

## The architectural idea, in one paragraph

Dagster's asset-graph + `DataVersion` is the right tool for incremental
re-runs (compute only what changed). Dagster's **push-based RunLauncher**
is *not* the right tool for >10k long-running LSF jobs — the daemon's
launch throughput becomes the bottleneck. So we split the concerns:
**Dagster owns lineage; an Execution Fabric we control owns execution.**
The asset body fires a non-blocking `bsub` and returns. A fabric worker
on the LSF node runs the script, computes the data version, and writes
SUCCESS to a status DB. A harvest sensor backfills materializations
into Dagster. Production storage is PostgreSQL; the in-repo demo uses
SQLite for single-host reference. Full design + rejected alternatives
in [`WHITEPAPER.md`](WHITEPAPER.md).

## Quick end-to-end check

```tcsh
setenv DAGSTER_HOME /tmp/fabric-demo
setenv LIBERATE_DAG_ROOT /tmp/fabric-demo/dag
setenv FABRIC_USE_MOCK_BSUB 1
setenv PYTHONPATH `pwd`/execution_fabric
python -m execution_fabric.scripts.run_demo
```
(bash: `export VAR=value`)

Expected (≤ 5 minutes): `Execution Fabric demo: PASS` with 9/9 SUCCESS
rows in the status DB, 9/9 `AssetMaterialization` events in Dagster,
9 `.ldb` artifacts on disk.

## Audience & next step

- **Porting an existing pipeline** → start at
  [`WHITEPAPER.md §5 Migration Plan`](WHITEPAPER.md#5-migration-plan)
  and switch the agent to `flow-cartographer`.
- **Looking up a Dagster 1.13.7 API** → switch the agent to
  `dagster-expert`; it reads `personalities/dagster-expert/database/dagster-1.13.7/`
  and refuses to answer from training memory.
- **Reading the design** → [`WHITEPAPER.md`](WHITEPAPER.md). The
  rejected-designs section at the bottom is the most useful entry
  point for anyone proposing changes.

## Air-gap stance

No internet at runtime. No `dg`/`uv`/Components/Dagster+/Cloud/k8s.
Wheelhouse pattern for pip. tcsh-first shell syntax (bash in parens).
Full deltas in
[`personalities/dagster-expert/database/dagster-1.13.7/docs/AIRGAP_DELTAS.md`](personalities/dagster-expert/database/dagster-1.13.7/docs/AIRGAP_DELTAS.md).
