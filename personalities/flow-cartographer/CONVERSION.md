# CONVERSION.md — the charter for the flow being converted

> This is flow-cartographer's **input surface**. The user writes it; the
> loop reads + executes it. It is the durable home of the requirements,
> so a weak model that loses context mid-task can re-read it every wake
> and not forget what it's doing or why.
>
> The `plan` tick decomposes this file into a ledger of increments
> (`flow-model/_plan.yaml`). The user no longer hand-writes increments.
>
> Replace every `[PLACEHOLDER]` below. The `reflect` tick may *propose*
> amendments (to `flow-model/_open_questions.yaml::charter_proposals`),
> but only the user edits this file. Direction stays human-owned.

---

## Flow identity

**flow_name**: [PLACEHOLDER — short slug, used for journal/L2 scope,
e.g. `real-char`]

**`$FLOW_SRC`**: [PLACEHOLDER — absolute path to the source flow on this
box. Set it: `setenv FLOW_SRC /abs/path` (tcsh) /
`export FLOW_SRC=/abs/path` (bash). Read-only; may not be under git.]

**Build target**: [PLACEHOLDER — where the Dagster project is built,
e.g. `~/projects/real-char-dagster`. `$DAGSTER_HOME` = `~/.dagster-cartographer`.]

---

## Goal (one sentence)

[PLACEHOLDER — what "converted" means here. Example: "Model the AP
real-char characterization flow (cell_list → lpe → corner_setup →
netlist_gen → liberate_run → liberty_aggregate → signoff_lib) as a
Dagster 1.13.3 asset graph with per-PVT fine-grain rerun and a folder-
as-asset contract, so an operator can re-run one failed PVT without
redoing the batch."]

> First real target is the **real-char pipeline** — the production form
> of `personalities/dagster-expert/learn/09-real-flow/`. That lesson is
> the canonical shape; read it as the reference DAG.

---

## Steps in scope

List the flow's steps (the `plan` tick will model each as
`flow-model/steps/<step>.yaml`). Mark any that are out of scope.

1. [PLACEHOLDER step, e.g. `cell_list` (Perl, root)]
2. [PLACEHOLDER, e.g. `lpe` (Perl, per-cell fan-out)]
3. ... add/remove as appropriate
- **Out of scope**: [PLACEHOLDER — steps to leave alone, e.g.
  "signoff_lib packaging, for phase 2"]

---

## Partition & source strategy

The conversion's hardest decisions, decided once, here:

- **Partition dimensions** (graph-theory names): [PLACEHOLDER, e.g.
  `corner` (root), `pvt`, `cell`]. Remember 1.13.3
  `MultiPartitionsDefinition` max 2 axes — say how extra axes collapse
  into composite keys.
- **PVT-/cell-partitionable sources to extract** (NOT copy): [PLACEHOLDER
  — list the hardcoded lists / near-duplicate source files that should
  become a `config/*.yaml` + generator. Example: "the PVT corner list in
  `cell_list.pl`; the per-cell netlist templates". These become L0
  increments — the loop must express them as config + generator, never
  copy them in.]
- **Tier boundary**: [PLACEHOLDER — which steps are Tier 1 (Dagster
  @asset, folder-as-asset) vs Tier 2 (per-step PVT fan-out stays in
  script). Default Tier 1 unless per-PVT control needs a script
  refactor that is in scope.]

---

## Constraints (non-negotiable)

- Dagster **1.13.3** only; every imported symbol must exist in
  `personalities/dagster-expert/database/dagster-1.13.3/docs/`.
- **No** `dagster._core/_internal/_private` imports.
- **Air-gap**: no `uv`/`dg`/`pipx`/Poetry/k8s/public PyPI/Docker.
- Target the **4-layer architecture** (spec → rules → registry →
  translator → factory) and the **folder-as-asset** contract, per
  `personalities/dagster-expert/demo/scale-lib/`.
- **Cardinality-math-first**: enumerate `branches × steps × cells × PVTs`
  before choosing a partition shape.
- [PLACEHOLDER — any extra constraint, e.g. "LSF dispatch via bsub
  wrapper, not local subprocess, for step 4".]

---

## Success criteria (the ralph-loop stop condition)

The conversion is "stable / done" when **all** hold (the `verify` and
`reflect` ticks evaluate these against the ledger):

1. Every in-scope step has a `done` Tier-1 asset increment (smoke-green).
2. Every PVT-/cell-partitionable source listed above is a `done` L0
   config+generator increment (no copied source files remain).
3. `python -m _smoke` (or the project's end-to-end driver) exits 0,
   materializing every partition through every in-scope asset.
4. Per-PVT fine-grain rerun works: materializing one partition key
   re-runs only that leaf (demo'd, like lesson 09 Demo 1).
5. No `status: blocked` increment and no `blocking: true` open question
   remains.
6. [PLACEHOLDER — optional domain criterion, e.g. "data_version
   propagation verified: changing one PVT's tool makes only downstream
   of that PVT stale".]

### When met

The next `verify`/`reflect` tick writes
`flow-model/digest/<date>-conversion-complete.md`, sets
`_plan.yaml::conversion_status: met`, and switches to polish-only mode
(no new build increments; reflect keeps running). The schedule is NOT
auto-stopped — you decide whether to extend the charter, point
`$FLOW_SRC` at a new flow, or retire it.
