# Why two tiers — the AP painpoints that justify the shape

This document is the **canonical motivation** for the scale-lib
demo + the two-tier architecture it implements. Authored from
Brian's 2026-05-12 articulation; replaces "Why this demo exists"
prose scattered across multiple READMEs.

## The three TSMC AP painpoints

| # | Painpoint | What's broken today |
|---|---|---|
| **1a** | **Fine-grain** | Per-PVT outputs not controlled by AP. Each step internally manages PVT; AP cannot reason about per-PVT staleness or per-PVT failure. |
| **1b** | **Grain (cross-library)** | Cross-library deps not modeled. User clicks AP per library; manual coordination, error-prone. |
| **2** | **No incremental / change event** | PVT list or source update → many steps don't support partial rerun → full rerun. Which steps to rerun = tribal knowledge + SOP. AP is opaque. |
| **3** | **No execution record** | AP runs leave nothing queryable for "what did production do last Tuesday". |

## What the original Dagster instinct missed

Lessons 09–11 (this corpus, 2026-05-08 → 10) leaned into
**fine-grain Dagster expansion**: model PVT as another partition
dimension, model cell as yet another, push every leaf into
Dagster's partition store.

That solves **#2 and #3** (Dagster gives you incremental + run
records) — but it doesn't solve **#1a** without significant Tier
2 refactor of step scripts. Worse, it makes Dagster look like a
silver bullet for #1 when it isn't; promising "Dagster will give
you per-PVT control" without the script refactor is empty.

## The two-tier resolution

```
Tier 1 (Dagster)         scope:    library × step (branch as partition)
                         contract: folder + data_version
                         "done" ≠ "no error";
                         "done" = folder digest verified
                         solves:   #2 (incremental) + #3 (record)
                                   + #1b (cross-library) — if library
                                     becomes a partition / code location

Tier 2 (per-step framework, not necessarily Dagster)
                         scope:    inside one step
                         contract: per-leaf done file + leaf-level deps
                         PVT (and cell) fan-out lives here.
                         Each script declares its own internal deps.
                         Tier 1 doesn't know Tier 2 exists; sees only
                         the step's output folder + its digest.
                         solves:   #1a (per-PVT control)
```

### The architectural win: the two tiers don't need to know about each other

This is **not just convenient — it's necessary** for adoption:

- Tier 1 can ship without touching step scripts (folder contract
  is enough) → low political friction
- Tier 2 refactor is **per-step opt-in**; no flag day required
- If a future framework replaces Tier 2 (snakemake / Nextflow /
  custom internal), Tier 1 is unchanged. The boundary is files,
  not API.

### Folder-as-asset is the contract that makes this work

A Tier 1 asset's "data version" is a SHA256 of the folder's
manifest — `(rel_path, size, int(mtime))` for every file. The
asset body's only job is:

1. Invoke the runner (`subprocess.run(["bsub", ..., script])`
   or `subprocess.run([script])` locally)
2. Verify the output folder exists and the digest is computable
3. Emit `MaterializeResult(data_version=digest, metadata={...})`

The runner can be Perl, Python, TCL, an EDA binary, an LSF job,
nested Dagster — Tier 1 doesn't care. **The folder is the
interface, not the script.**

#### Who computes the digest?

For LSF-dispatched steps, the digest should be computed
**node-side**, not on the Dagster host. The LSF wrapper writes
`<out>/.dagster_meta.json` as its last action before exit;
Dagster reads that JSON. Reasons:

| | Central (Dagster host stat over NFS) | Node-side (wrapper writes manifest) |
|---|---|---|
| 50k-file folder | 30s–2min per materialization | ~1s |
| Trust boundary | Dagster computes everything | Dagster trusts node — same trust as trusting the script ran |
| NFS round-trips | High | Just one (read `.dagster_meta.json`) |

For local-only steps (no LSF), central is fine — local FS stat
is fast.

See `CONTRACT.md` § "Who computes the folder digest" for the
implementation recipe.

## Phased adoption (Brian's plan)

Don't roll out everything at once. Three independent validation
cycles:

| Phase | Scope | Validates |
|---|---|---|
| **Observer** (cheap, ~1 week) | Tier 1 watches existing AP via `observable_source_asset` on touch files. No scheduling. AP keeps running unchanged. | Folder digest stability across real production runs. UI as execution record (#3 solved immediately, zero risk). Lesson 17 demos this. |
| **Step take-over** (medium, ~1 month) | Tier 1 actively runs ONE chosen step (highest incremental pain). AP touch file becomes a Tier-1-emitted side-effect for backward compat. | Operator error rate change. Engineer trust. Folder-digest correctness on real outputs. |
| **Cross-library + Tier 2** (long, several months) | Library becomes 2nd partition dim OR separate code location. Tier 2 framework for 1–2 selected steps where per-PVT pain is severe. | #1a fine + #1b grain. The full vision. |

Don't skip phases. Each one can be aborted independently if it
doesn't pay off.

## What scale-lib demonstrates today

`personalities/dagster-expert/demo/scale-lib/` is **Tier 1 of
this vision, validated end-to-end** (Brian, 2026-05-12; 81/81
unit + 18/18 UI GraphQL tests pass; `python -m _smoke`
materializes 16 partitions; UI shows 23 assets across 5 groups).

What's there:
- 21 step assets per library × 46 branch partitions
- Folder-as-asset contract via `folder_digest.digest_folder_manifest()`
- 4-layer dep architecture (`spec/` → `rules/` → `registry.py` →
  `translator.py` → `factory.py`) so dep facts live in one place
- `runners.py` as the swap point: change `subprocess.run([script])`
  to `subprocess.run(["bsub", "-K", "-J", ..., script])` for LSF
- `pvt_manifest` `observable_source_asset` (seed for the
  PVT-change-driven incremental story)

What's still **explicitly** absent from scale-lib (each by
design, deferred to later phases):
- Library dimension (only `lib_a` modeled)
- Real LSF integration (subprocess runner is local-only)
- Tier 2 framework for any step
- A step that actually depends on `pvt_manifest` (this gap is
  fixed in lesson 17 / the same commit set as this doc)

## Open risks (collected from review)

1. **Who owns Tier 1 config?** Operating team owns it (per Brian
   2026-05-12). Implies: changing a step's dep rule requires a
   ticket / review by that team. If too gated, AP engineers
   adding new steps gets slow; if too open, the consolidated dep
   model can drift back to the chaos Tier 1 is meant to fix.
2. **Folder-digest cost** — node-side computation (above)
   addresses LSF case. Pure local-host case at 50k+ files still
   slow (5-10s); document so users know to expect it.
3. **Manual override** — when a user hand-edits a folder, Tier
   1 sees "changed" and wants to rerun. Need a sentinel (e.g.
   `.dagster_no_rerun` in the folder) to say "I own this; don't
   touch". Not yet implemented in scale-lib.

## References

- `personalities/dagster-expert/demo/scale-lib/` — Tier 1 reference impl
- `personalities/dagster-expert/demo/scale-lib/CONTRACT.md` — Tier 1/2 boundary spec
- `personalities/dagster-expert/learn/17-observer-mode/` — observer-mode prototype
- Earlier _inbox: `2026-05-12T095806-claude-two-tier-orchestration.md`
  (the technical version of the same insight)
- Earlier _inbox: `2026-05-12T220000-tl32rodan-two-tier-pain-driven-framing.md`
  (the inbox draft this document promotes)
