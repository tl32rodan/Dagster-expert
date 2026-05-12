---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-12T22:00:00+0800
tags: [architecture, ap, tier, dagster, change-event, incremental, painpoint, brian-framing]
---
# Why Dagster — the actual TSMC AP pain points that justify the two-tier shape

## Brian's framing (2026-05-12, verbatim distillation)

The current AP system has three painpoints. Earlier lessons 09–12
over-focused on the wrong one (#1 fine).

| # | Painpoint | What's broken |
|---|---|---|
| 1a | **Fine-grain** | Per-PVT outputs not controlled by AP — each step internally manages PVT. AP can't reason about per-PVT staleness. |
| 1b | **Grain (cross-library)** | Cross-library deps not modeled — user must manually click AP per library, error-prone. |
| 2 | **No incremental / change event** | PVT list or source update → many steps don't support partial rerun → full rerun. Which steps to rerun = tribal knowledge + SOP, not a property of the system. |
| 3 | **No execution record** | AP runs leave nothing queryable for "what did production do last Tuesday". |

**The insight**: fine-grain Dagster expansion (the original instinct
behind lessons 09–11) solves only #2 + #3. It does NOT solve #1
without significant Tier 2 refactor of step scripts. Bidding for #1
without Tier 2 = empty promise.

## The two-tier resolution

```
Tier 1 (Dagster)     library × step (branch as partition)
                     contract: folder + data_version
                     "done" ≠ "no error"; "done" = folder digest verified
                     solves #2 (incremental) + #3 (record) + #1 grain (cross-library)

Tier 2 (per-step framework, NOT necessarily Dagster)
                     PVT expansion lives here
                     each script declares its own internal deps
                     Tier 1 doesn't know Tier 2 exists; sees only folder
                     solves #1 fine
```

**The win that makes this architecturally clean**: **the two tiers
don't need to know about each other**.

- Tier 1 can adopt without touching scripts (folder contract is enough)
- Tier 2 refactoring can be **per-step opt-in** (no flag day)
- If a future framework replaces Tier 2 (snakemake / Nextflow /
  custom), Tier 1 is unchanged
- Backward compatibility: Tier 1 can **observe** existing AP via
  touch-file ⇄ `observable_source_asset` mapping; full take-over
  is incremental

## Validates scale-lib's existing shape

`demo/scale-lib/` already implements Tier 1 of this vision:
- 21 step assets per library × 46 branch partitions
- `folder_digest.digest_folder_manifest()` is the contract enforcer
- `runners.py` is the swap point — flip subprocess to bsub for
  LSF Tier-2 without touching step assets
- `pvt_manifest` `observable_source_asset` is the seed for
  PVT-change-driven incremental rerun

The library dimension and Tier-2-as-Dagster aspects are NOT in
scale-lib yet — they're explicit follow-ups in Brian's plan.

## Suggested phased adoption (Brian's plan, my elaboration)

| Phase | Scope | Validates |
|---|---|---|
| **Observer** (cheap) | Tier 1 watches AP via `observable_source_asset` on touch files; no scheduling | Folder digest stability; UI as execution record (#3 solved) |
| **Step take-over** (medium) | Tier 1 actively runs 1 chosen step (highest incremental pain); AP touch file becomes Tier 1 side-effect | Operator error rate; trust |
| **Cross-library + Tier 2** (long) | library as 2nd partition dim OR code location; Tier 2 framework for 1-2 steps | #1a fine, #1b grain |

**Don't**: jump straight to full Tier-1 + Tier-2 + cross-library
+ cross-branch. Three independent validation cycles, each
failable separately.

## Open questions (collected from author + the agent's review)

1. **Who owns Tier 1 config?** If review-gated, AP engineers adding
   a step gets slower — political pushback risk.
2. **PVT list change → Tier 1 staleness propagation**: only works if
   PVT list is an `observable_source_asset` AND every step that
   depends on it lists it as `deps=[AssetKey("pvt_manifest")]`.
   Currently scale-lib has the source but no step depends on it.
3. **Folder-digest cost at scale**: O(stat) over 50k+ output files
   per step is 5–10s. Acceptable but should be measured + documented.
4. **Manual override**: when user hand-edits a folder, Tier 1 sees
   "changed" and wants to rerun. Need an `.dagster_override` sentinel
   or similar to say "agent: I own this, don't touch".

## Curator action

Promote to a new top-level entry under
`personalities/dagster-expert/memory/understanding/why-two-tier.md`
(or merge into `scale-lib-demo.md`'s rationale section). The
TSMC-specific framing (3 painpoints, what scale-lib already
addresses, what's still ahead) deserves to be canonical context for
the demo — current `scale-lib-demo.md` describes architecture but
not motivation.

Cross-reference from:
- `learn/12-scaling/README.md` § "Why this lab exists" (add a
  pointer up to the two-tier framing)
- `demo/scale-lib/README.md` § "Why this demo exists" (replace the
  current rationale with this 3-painpoint articulation)

## References

- This conversation, 2026-05-12 evening session
- `demo/scale-lib/CONTRACT.md` (folder-as-asset contract — Tier 1/2 boundary)
- `demo/scale-lib/pipelines/source_observers.py` (the `pvt_manifest` seed)
- `learn/16-hooks-automaterialize/` (observable_source_asset mechanics)
- Prior _inbox: `2026-05-12T095806-claude-two-tier-orchestration.md` (the
  technical version of the same insight, before Brian articulated the
  TSMC-specific painpoints that drive it)
