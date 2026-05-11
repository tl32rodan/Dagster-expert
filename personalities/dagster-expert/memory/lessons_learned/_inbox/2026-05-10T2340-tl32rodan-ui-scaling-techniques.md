---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-10T23:40:00+0800
tags: [ui, lineage, key_prefix, group_name, scalability]
---
# UI scaling: `key_prefix` + `group_name` over partition collapse

## Observation

When a flow has many parallel sub-flows (Brian: "branch 最新進展
已經有時會到達 30 幾個"), the lineage UI gets unworkable. Tempting
fix: collapse parallel branches into a partition axis (fewer
asset nodes). This is the WRONG lever.

## Right lever: visual grouping, not semantic collapse

| Lever | Effect | When |
|---|---|---|
| `key_prefix=[<group>]` | Asset key namespaced as `group/name`; UI lineage view shows nested folders | Always when you have multi-instance asset families |
| `group_name="<name>"` | Assets share a colored group; lineage UI's "Groups" view collapses each group_name into one expandable node | Many assets; want the high-level shape first |
| **Code locations** (separate `workspace.yaml` entries) | Each location is its own gRPC server / process; lineage UI shows them as super-groups | Hundreds+ of assets, or different teams/versions |

## Why partition-collapse is the wrong lever for this problem

Folding "branch" into a partition axis (e.g. `(branch, pvtrc)`)
saves N×3 assets but:
- **Loses cross-branch dep visibility** in lineage (Dagster
  doesn't see deps you implement via filesystem in body)
- **Per-partition pipelining is unchanged** — that's identity
  mapping default, not affected by asset count
- **Stale propagation breaks** — invisible deps mean Dagster
  can't compute downstream staleness

So it solves a UI problem by hiding semantics. Don't.

## Production-scale numbers

- 540 assets (6 libraries × 30 branches × 3 steps): manageable
  via group_name + key_prefix in a single code location
- 1000+ assets: probably want code-location split per library
- 10000+ assets: that's where Dagster Cloud / gRPC at scale
  becomes architectural

## Curator action

Add a new cheatsheet entry,
`dagster-librarian/database/dagster-1.13.3/docs/ui-scaling.md`,
covering:

- The trio of techniques: `key_prefix`, `group_name`,
  code locations
- A worked example (similar to lesson 11) showing 24 assets
  grouped into 9 group_names in a single workspace
- Decision tree: when to group_name, when to split into code
  locations
- The anti-pattern (partition collapse) and why it doesn't
  solve the visualization problem

Reference from `asset-basics.md` (under @asset decorator args).
