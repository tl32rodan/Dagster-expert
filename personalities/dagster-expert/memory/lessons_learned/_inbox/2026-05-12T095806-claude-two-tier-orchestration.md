---
allmight_journal: v1
type: lesson_learned
submitter: claude
created_at: 2026-05-12T09:58:06+00:00
tags: [scaling, architecture, dagster, lsf, tier, two-tier, eda]
---
# Two-tier orchestration is the correct response when leaf cardinality breaks the partition store

## Observation

User asked whether Dagster could handle a real workload:
46 branches × 21 steps × ~100 cells × 3000+ PVT combinations
= ~13.8M leaves at full resolution.

I almost proposed making cell + PVT third + fourth partition dimensions
on top of branch. Was forced to stop and check:

- Dagster 1.13.3 `MultiPartitionsDefinition` limit: 2D. So branch × cell
  alone uses all dimensions.
- Even if 1.13.4 lifted that, the partition store (SQLite/PG) is OLTP-style;
  not designed for millions of partition rows per asset.
- Same constraint applies to Airflow task instances, Prefect flow runs,
  Argo workflow nodes. Not a Dagster-specific limit.

The industry pattern for million-leaf workloads (especially EDA): **two-tier
orchestration**.

```
Tier 1 (upper)  general-purpose orchestrator (Dagster/Airflow/...)
                sees: (library, branch, step) — ~1k–10k partition records
                provides: lineage, retry, schedule, UI, observability

Tier 2 (lower)  batch scheduler (LSF / Slurm / custom)
                sees: per-(cell, PVT) leaf jobs — 10k–10M per run
                provides: real horizontal parallelism, resource scheduling
```

TSMC's natural fit for Tier 2: **LSF** (Cadence Liberate, Synopsys SiliconSmart
integrate with it natively). Tier 1's asset body submits `bsub -K -J ... script`
and blocks; rest of pipeline is identical to local subprocess.

## Why this matters

When the user has a real "Dagster can't handle this scale" reaction, the
fix is rarely **switch frameworks** and almost never **add more partition
dimensions**. It is **move leaf-level work into Tier 2**.

The architectural property that makes this work: Tier-1 asset is a *folder*,
not a *file*; its data_version is the folder's sha256 manifest. The asset
body invokes a runner that submits LSF and waits. The Tier-2 fan-out is
opaque to Dagster — and that opacity is the point.

## Decision tree for "this won't scale"

1. Compute leaf cardinality (branches × steps × cells × PVTs × ...).
2. If ≤ ~10k partition records total → Dagster handles it alone, no tier 2.
3. If > ~10k but the inner dim has no cross-dep structure → push the inner
   dim to Tier 2 (script + LSF / Slurm). Most cases land here.
4. If the inner dim DOES have its own DAG → consider nested Dagster (Tier-2
   instance per Tier-1 node, ephemeral). Heavy; only when memoization +
   retry-by-leaf is worth the wheelhouse / bootstrap cost.
5. If the Tier-1 cardinality is itself > ~10k → split into multiple code
   locations (lesson 11 pattern), one Dagster instance per logical grouping.

## What I almost did wrong

Initially recommended branch × cell as a 2D MultiPartitionsDefinition for
step1/step4 with PVT going inside the script. That was already a tier-cut,
but I framed it as "PVT is a third dim we can't add yet" rather than
"PVT belongs in Tier 2 by design." The user's question "Is dagster even
right?" forced the reframe.

Lesson: **be explicit about where the orchestrator stops and the batch
system begins**, even when both layers are subprocess-based and live on
the same machine. The cardinality math should drive the boundary, not the
framework's API surface.

## References

- `personalities/dagster-expert/demo/scale-lib/CONTRACT.md`
- `personalities/dagster-expert/demo/scale-lib/README.md` § Architecture
- `personalities/dagster-expert/learn/13-lsf-integration/`
