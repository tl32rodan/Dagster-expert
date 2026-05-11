---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-10T23:35:00+0800
tags: [partitions, partition-mapping, pipelining, runtime, scheduling]
---
# Per-partition pipelining: step.N+1 partition X doesn't wait for step.N partition Y

## Observation

Brian asked whether collapsing branch into a partition axis
forces "all step-N partitions must finish before any step-(N+1)
partition starts". The answer is NO, but it's not in the
cheatsheet currently.

## Behavior (Dagster 1.13.3)

Default `IdentityPartitionMapping` makes each partition its own
independent execution unit:

- `step2 (partition=tt_25)` only waits for `step1 (partition=tt_25)`
- It does NOT wait for `step1 (partition=ff_125)` or
  `step1 (partition=ss_m40)` to finish
- Backfills can pipeline: `step1[tt_25] → step2[tt_25]` runs while
  `step1[ff_125]` is still in flight
- This is the whole point of partitioning vs. monolithic runs

The same holds for MultiPartitionsDefinition: identity mapping is
key-by-key, dimension-by-dimension.

## When it doesn't hold

- **`AllPartitionMapping`** (explicit) — downstream partition X
  fans in over ALL upstream partitions. Forces wait.
- **Custom `PartitionMapping`** with all-to-one semantics — same.
- **Asset-level dep without partition mapping** (e.g. unpartitioned
  downstream depends on partitioned upstream) — Dagster collapses
  upstream partitions into one input; effectively all-wait at
  the asset level.

## Why this matters for AP characterization

The fine-grain split + per-partition-pipelining property is what
makes "fail one PVT, re-run only that one" possible. Without
identity mapping, fixing one failed PVT would re-run the whole
partition family at the next step.

## Curator action

Add a section to
`dagster-librarian/database/dagster-1.13.3/docs/partitions.md`
called "Per-partition pipelining (default) — and when it
doesn't hold". Show:
- Default IdentityPartitionMapping behavior
- Visual: matrix of (asset × partition) showing which units
  block which (single-cell wait, not full-row wait)
- The exceptions (AllPartitionMapping, custom mappings,
  asset-level deps that drop partition mapping)
- Implications for backfill parallelism + incremental rerun
