---
name: build-loop
description: >
  Convert EXACTLY ONE ledger increment to Dagster 1.13.3. Includes the
  source→config transform for PVT/cell-partitionable sources. Produces
  code, marks the increment `built` (never `done` — the verify tick
  promotes it). Runs on the `build` tick.
---

# build-loop — convert one increment

The Wake SOP already ran pre-flight and read the handoff. You build ONE
increment this tick. You do not verify it (that is the next, separate
tick, on purpose — a different framing catches your own mistakes).

## Step 1 — Pick the increment

From `_plan.yaml`, pick the **lowest-id `planned` increment whose every
`depends_on` is `done`**. This is normally what `STATUS.md::next_action`
points at; if they disagree, trust the ledger and note it in
`_operations.log`. If none is ready (all `planned` are blocked on deps
or open questions), write a `noop` handoff explaining why and stop.

Set the picked increment's `status: building` and journal "building
<id>: <title>" before touching code.

## Step 2 — Load the increment's context (no guessing)

Read, in this order:

1. The increment's `converts:` source at `$FLOW_SRC/<file>` — the exact
   thing you are converting. Read it; do not assume its behavior.
2. The step node `flow-model/steps/<step>.yaml` — partition dims,
   tier, fan shape.
3. The sibling ground truth for the pattern you need:
   - `personalities/dagster-expert/demo/scale-lib/` for the 4-layer
     architecture (spec → rules → registry → translator → factory).
   - `personalities/dagster-expert/learn/09-real-flow/` for the
     asset / Pipes / checkpoint / fan-in patterns.
4. **Before writing ANY `from dagster import …` line**, run the
   sibling's API lookup: `personalities/dagster-expert/skills/lookup-api/SKILL.md`.
   0 results for a symbol ⇒ do NOT write it; park an open question.

## Step 3 — Build (by `target_layer`)

Build only this increment. Match scale-lib's import-boundary rule
(layers 0–2 import no Dagster; 3–4 do).

- **L0 — source→config transform** (the priority case). The source is a
  hardcoded per-PVT / per-cell list or N near-duplicate files. Convert
  it to: (a) a settings/config file (`config/<name>.yaml`) holding the
  partition values / relationships, and (b) a small generator that
  writes the per-leaf files the original flow expected. The generated
  files must correspond to what `$FLOW_SRC` produced. **Do NOT copy the
  source files in.** If you find yourself about to copy a file
  unchanged, stop — that is the failure mode; express it as config +
  generator instead.
- **L1/L2 — spec/rules/registry**: pure-data dep facts + composition.
  No Dagster import. Mirror `pipelines/spec/`, `pipelines/rules/`,
  `registry.py`.
- **L3 — translator**: `PartitionRule` → `StaticPartitionMapping`.
- **L4 — asset / runner**: one step → one partitioned `@asset` with the
  folder-as-asset contract (data_version = folder digest). Plain
  `subprocess.run([...])` for loose/legacy steps; `PipesSubprocessClient`
  for tightly-integrated steps that stream events (see lesson 09).

Write a minimal `accept` smoke target if one doesn't exist yet (e.g.
extend the project's `_smoke.py`) so verify has something to run.

## Step 4 — Record what you used, mark `built`

In `_plan.yaml` for this increment:
- fill `dagster_api:` with the public symbols you actually imported
  (verify will confirm each exists in the corpus).
- set `status: built`.

Do NOT set `done`. Do NOT commit as final. The verify tick promotes
`built → done` (or sends it back to `blocked`).

## Step 5 — Handoff and stop

Per Wake SOP Step 4: append `_operations.log`
(`<ts> build <id> built <one-line>`), update `STATUS.md`
(`active_increment: <id>`, `next_action: verify <id> (built, awaiting
self-check)`), journal the conversion with the `$FLOW_SRC` citation and
the Dagster APIs used. One increment, then stop.
