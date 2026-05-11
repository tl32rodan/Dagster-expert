---
date: 2026-05-09
unix_user: tl32rodan
session: dagster-tutor walkthrough Lesson 03
status: inbox — pending curator review
---

# Leaf assets have no "stale" state

## Observation (Brian, Lesson 03 walkthrough)

In a single-asset setup (e.g. Lesson 03's `corner_summary` —
partitioned by 4 corners, no downstream), users may expect that
editing the asset source + reload makes the asset "go yellow".
**It doesn't.** No matter what you do to the source, a leaf asset
only has two visible states in the UI:

- **Gray** — never materialized for this partition
- **Green** — successfully materialized for this partition

There is no **yellow** ("stale") state because staleness is defined
as "downstream found upstream's stored data_version different from
what it last consumed". A leaf asset has no consumer, therefore no
comparison can be made.

## Why this is non-obvious

The `code_version` / `data_version` literature emphasizes the
"upstream → downstream" propagation chain. New users intuit that
"changing code should propagate something" — but for a leaf
asset, the propagation has nowhere to go. The intuition is correct
in spirit, just inapplicable.

## What the cheatsheet should say

Add to `partitions.md` (or `data-version-and-staleness.md`) a
short note:

> **Leaf assets don't go yellow.** Staleness requires a downstream
> consumer to compare upstream's stored `data_version` against
> what it last consumed. A leaf (no `deps=`, no function-arg
> upstream) has no comparator → it's only ever gray or green.
>
> To demonstrate per-partition staleness, you need a chain:
> partitioned upstream → downstream that depends on it. Then
> editing one upstream partition + re-materializing makes the
> downstream-of-that-partition go yellow.

## Affects

- Lesson 03's "Now-try" exercises — Try 1 originally claimed
  reload would make a partition go yellow. This is wrong twice:
  (1) reload-alone never propagates staleness without explicit
  `code_version`, (2) leaf assets have no yellow regardless.
  Already corrected in conversation.
- Lesson 02's "Now-try" was also looser than it should be re:
  reload-alone behavior, though the chain DOES eventually go
  yellow if you re-materialize.

## Curator action

Promote to `partitions.md` as a **"Common gotchas"** entry
called "Leaf assets have no yellow / stale state". Cross-reference
from `data-version-and-staleness.md`'s "Diagnosing a broken
propagation chain" section ("if you don't see yellow, first
check whether the asset HAS a downstream consumer").

Also: consider adjusting Lesson 03's README.md to make this
explicit so it's not a surprise during walkthrough.
