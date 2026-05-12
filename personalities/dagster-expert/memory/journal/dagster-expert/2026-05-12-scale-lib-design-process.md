---
allmight_journal: v1
id: 2026-05-12T09:58-scale-lib
type: decision
workspace: dagster-expert
trigger: slash_remember
input: |
  User asked to record the decision/exploration process behind the
  scale-lib demo, where lesson 12 was deemed too small for real
  TSMC AP characterization scale.
tool_calls: []
output: |
  Captured the full pivot from "Dagster as one-tier finegrain partition
  store" to "Dagster as Tier-1 over folder-as-asset / LSF Tier-2" plus
  the 4-layer dep-centralization architecture.
outcome_label: success
tags: [decision, scaling, two-tier, dep-architecture, terminology, dagster, tsmc-ap]
supersedes: null
created_at: 2026-05-12T09:58:00+00:00
---

# 2026-05-12 — scale-lib demo: design decisions & pivots

Multi-hour planning + implementation session that produced
`personalities/dagster-expert/demo/scale-lib/` (76 files, 2975 lines, 89
tests pass). The decisions that mattered most are below in roughly
chronological order; many were course-corrections that future sessions
should replay before re-deriving from scratch.

## Pivot 1 — Lesson 12 cardinality vs production reality

Lesson 12 demos 2 libraries × 4 branches × 5 steps × 3 PVTRCs ≈ 120
partition records. User's actual workload:

- 46 branches (was stated as 64; enumerated to 46 — user confirmed 46
  is fine, 64 was rough guess)
- 21 logical steps (2 setup + 4 extraction + 6 char + 9 kits)
- ~100 cells per library
- 3000+ PVT combinations (dual/triple voltage sweep)

Naive total: 46 × 21 × 100 × 3000 ≈ **290M leaves**.

This kicked off the "is Dagster even the right tool" question.

## Pivot 2 — 1.13.4 corpus check

User mentioned 1.13.4 might be available internally. Librarian check:
**no 1.13.4 docs indexed**; only 1.13.3. I refused to invent changelog
entries. Decision: stay on 1.13.3, README marks places where 1.13.4
**might** simplify (e.g. MultiPartitions 2D limit) but does not claim it.

## Pivot 3 — PVT as partition? NO

First instinct was to add PVT as a third partition dimension. Blocked by:

- 1.13.3 MultiPartitions hard limit: 2D
- Even if 4D were allowed, 46 × 100 × 3000 partition records per asset
  is well beyond what SQLite or PG handle gracefully

Three strategies considered:
- A: PVT entirely script-internal (recommended)
- B: PVT_corner (3-9) as second dim, spec list in script
- C: PVT_batch (~30) as second dim

User went with "鮮 focus on 1.13.3" via cardinality-driven argument.

## Pivot 4 — Framework re-evaluation (the big one)

User: "is that true? 業界類似規模怎麼解決? 需不需要轉框架?"

This was the hardest moment. Honest answer:

- The constraint isn't Dagster-specific. Airflow / Prefect / Argo all
  have the same partition / task-instance ceiling at similar order of
  magnitude.
- Industry standard for million-leaf workloads is **two-tier
  orchestration**: orchestrator above (Dagster/Airflow), batch scheduler
  below (LSF / Slurm / SGE).
- EDA shops use LSF natively (Cadence Liberate, Synopsys SiliconSmart
  integrate with bsub).
- Frameworks that handle 100k+ rules natively (Snakemake, Flyte) trade
  the UI / observability story.

Conclusion: **don't switch frameworks**. The dagster-expert investment
(12 lessons, librarian, memory) stays valuable. The fix is to move PVT
+ cell out of the Dagster topology and into the step script (Tier 2,
later switched to LSF bsub).

## Pivot 5 — Nested Dagster considered, rejected for v1

User proposed Tier-2 Dagster nested inside each Tier-1 LSF job, with
folder-as-asset at Tier 1 and file-as-asset at Tier 2. Analyzed:

- Technically viable; each instance gets ephemeral `DAGSTER_HOME` on
  scratch
- Per-Tier-1 node sizing: PVT × cell = 3M per node — still too big
  for a single Dagster instance
- Need to fan out further (Tier 3) or pick a single dim per Tier 2
- Wheelhouse + bootstrap on every LSF host is operational overhead

User's leaf "通常沒有太多 cross-PVT dependency" → most of Dagster's
value (lineage) doesn't apply at leaf. Plain script + LSF array is
sufficient.

Decision: **Phase 0 = Tier 1 only**. Tier-2 Dagster deferred until
memoization or cross-PVT deps justify it. README documents the upgrade
criteria.

## Pivot 6 — Real pain-point taxonomy

User's actual production frustration (forced clarity on what we're
solving):

1. fine-grain (per-PVT control inside a step) — Tier 2's job
2. grain (cross-library deps) — Tier 1's job
3. incremental rerun / change events — Dagster staleness propagation
4. no execution trace — Dagster run history

Tier 1 alone solves 2/3/4 + "grain" half of 1. The "fine" half is
deferred indefinitely.

## Pivot 7 — branch-as-partition + custom PartitionMapping

User asked: "If branch is partition, do independent branches block each
other?"

Answer: No, as long as PartitionMapping is correct.
- Intra-branch chain: `IdentityPartitionMapping` (default)
- Cross-branch parent mirror: custom mapping that resolves downstream's
  parent + self

This pulled the design back from "branch as independent asset" (would
produce 46 × 21 = 966 declarations) to "branch as partition with rich
PartitionMapping" (21 assets × 46 partitions).

## Pivot 8 — Centralize dep facts (the architectural shift)

User: "目前你的 code 與現存的 lessons 都沒有很乾淨地把 dep 集中定義
而是分散在各個 decorator 與 sub function 中"

Major refactor. Output: 4-layer architecture documented in
`memory/lessons_learned/_inbox/2026-05-12T095807-claude-centralized-deps-4layer.md`.

Key insight: lessons 09–12 use the Dagster-idiomatic inline-deps style
because they teach the API. Production demos need stricter discipline
because real workflows have dozens of rules + override needs.

## Pivot 9 — Terminology shift to graph theory

User: "可以改用更符合 dag/ graph theory 的用語" (avoid EDA-specific
"corner" for the role/relationship; the branch named "corner" stays).

Substitutions made:
- `corner_of(b)` → `parent_of(b)`
- `is_corner(b)` → `is_root(b)`
- `CornerMirrorRule` → `ParentMirrorRule`
- `CornerOfDownstream` → `ParentOfDownstream`
- The specific branch named ``corner`` keeps its name (it IS the root,
  but ``root`` would be confusing since it's also a tree concept).

## Implementation surprises

Hit several Dagster 1.13.3 API gotchas documented in
`memory/understanding/dagster-1.13.3-gotchas.md`:

1. `from __future__ import annotations` breaks `@asset` context validation
2. `@asset(key=...)` conflicts with `name=` / `key_prefix=`
3. `ins=` triggers IO loading; switch to `deps=[AssetDep(...)]`
4. Custom PartitionMapping is deprecated for reconciliation; pre-compute
   `StaticPartitionMapping` via branch enumeration
5. `PartitionsSubset` needs internal import
6. `Definitions.get_asset_graph()` → `resolve_asset_graph()`,
   `.all_asset_keys` → `.get_all_asset_keys()`
7. `AssetKey.path` is list (unhashable in sets)

Each cost 1–5 minutes to diagnose. The understanding doc should save
that next time.

## Final verification

- 89/89 tests pass: 81 unit/integration (sub-second) + 8 GraphQL UI
- End-to-end materialization works (5 branches × 4 steps via _smoke.py)
- Backfill via GraphQL mutation `launchPartitionBackfill` succeeds
- Parent-mirror semantics verified: step5[tmsf_lde1] materialized only
  after step4[tmsf_lde1] AND step4[tmsf_self] both done

## What I would do differently

- **Cardinality math first.** I should have run the 290M-leaf
  calculation in the first reply instead of getting halfway into the
  4-D-partition rabbit hole.
- **Ask "where does Dagster stop?" earlier.** The two-tier framing
  was the clarifying lens. Should be the second question after "what
  scale?"
- **Don't enumerate "CORNER_MERGE_STEPS" as a precondition.** It is a
  rule parameter, not a design-blocking question. User flagged this.

## Open threads for future sessions

- AP `.ap_done` compatibility shim (sensor + post-step touch hook)
- Second library code location (lesson 11 multi-loc pattern)
- Real LSF bsub wiring on `PerlRunner.run` (one-line swap)
- Tier-2 Dagster experiment (only if leaf memoization pain emerges)
- Re-evaluate plan when 1.13.4 corpus arrives (drop release notes
  into `database/dagster-1.13.4/`, run `/ingest`)
