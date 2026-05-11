---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-10T23:30:00+0800
tags: [eda, terminology, partition, dependency, ap-characterization]
---
# "Branch" in EDA AP characterization ≠ corner / partition value

## Observation

In a Dagster lesson walkthrough modeling TSMC AP characterization,
the agent (me) initially modeled "branches" as cross-corner deps
(ff/ss derive from tt). Brian corrected:

> "pvtrc (e.g. ff/tt/ss 這些是 rc corner) 並不是 branch. 我們常見的
> branch 有 corner/lvf/em. 他們都會有不同集合的 pvtrc list, 共通點
> 是每一步的 lvf/em 都 depend on corner 跟前一步的 lvf/em ; 而每步
> 的 corner 都只 depend on 前一步的 corner"

So in TSMC AP characterization terminology:

- **PVTRC** (process-voltage-temperature-rc-corner) = partition values.
  ff_125, tt_25, ss_m40 are points along this axis.
- **Branch** = orthogonal characterization channel. Each branch
  produces a different deliverable type:
  - `corner`  — standard timing/power Liberty tables
  - `lvf`     — Liberty Variation Format / statistical timing
  - `em`      — Electromigration constraints

Each branch has its OWN PVTRC sub-list (different ops points where
that characterization technique applies):
- corner: full sweep
- lvf: typical only (statistical doesn't need corners)
- em: extremes only (stress-driven)

## Cross-branch dep pattern

```
corner.N depends on corner.(N-1) only          — branch self-contained
lvf.N    depends on (corner.N at same PVTRC, lvf.(N-1))
em.N     depends on (corner.N at same PVTRC, em.(N-1))
```

Critical: the cross-branch dep is **same-step** + **same-PVTRC
matching** (lvf @ tt_25 reads corner @ tt_25, not corner @ ff_125).

## Why the cheatsheet should record this

LLM agents (including me) defaulted to interpreting "branch" via
software-engineering meaning (parallel paths in a graph =
git-style branches). In EDA flows the term has specific structural
meaning that matters for partition design and dep modeling.

## Curator action

Promote to a new cheatsheet entry,
`dagster-librarian/database/dagster-1.13.3/docs/eda-branch-pattern.md`,
covering:

- Branch terminology (corner / lvf / em) and what each emits
- Per-branch PVTRC sub-list pattern
- Cross-branch dep recipe (same-step, same-PVTRC match)
- Implementation choice: per-branch separate assets (Style B
  filesystem fan-in across branches) vs. branch-as-partition-axis
  (collapses dep visibility, not recommended)

Cross-reference from `partitions.md` and `style-a-vs-b.md`.
