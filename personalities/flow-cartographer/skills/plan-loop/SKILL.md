---
name: plan-loop
description: >
  Task-decomposition standard. Turn a given execution flow into a
  modeled DAG plus an ordered ledger of small, independently verifiable
  conversion increments. Runs on the `plan` tick (and re-runs each
  reflect tick to re-evaluate).
---

# plan-loop — the task-decomposition standard

One `plan` tick does ONE of: bootstrap the model+ledger (first time),
or refresh them (charter changed, or reflect asked). It never builds
code. Output: `flow-model/steps/*.yaml` + `flow-model/_plan.yaml`.

Honor `MEMORY.md` user prefs throughout: **cardinality-math-first** and
**graph-theory terms** (`root`, `parent_of`, `ancestors_of` — never
`corner_of`). Read the sibling's `demo/scale-lib/` + `learn/09-real-flow/`
as the shape you are converting toward.

---

## Step 1 — Model the flow (understand before planning)

Walk `$FLOW_SRC` (Read / `grep -rn`). For each step in the flow, write
one node `flow-model/steps/<step>.yaml` per the `_schema/step.yaml`
schema. Capture, per step:

- `cmd` / `script` + `lang` (perl | python | tcl | shell | other)
- `inputs` / `outputs` (files / folders; how outputs are named)
- `partition_dims` it is run across (e.g. `corner`, `pvt`, `cell`)
- `pvt_partitionable` / `cell_partitionable` (bool) — CAN its source or
  invocation be split per-PVT / per-cell?
- `fanout` / `fanin` shape (per-cell fan-out, by-corner glob fan-in, …)
- `sources` — config/data files this step reads that are themselves
  partitionable (the candidates for the source→config transform)

If a step's behavior is unclear, DO NOT guess: add an entry to
`flow-model/_open_questions.yaml` and mark the step `model: partial`.

State the DAG back as a short ASCII sketch in the journal (like
`learn/09-real-flow/README.md`'s DAG), so a human can sanity-check the
model before increments are built.

---

## Step 2 — Map each step to the two-tier target

For each step decide its tier (record `tier:` on the step node):

- **Tier 1 (Dagster)** — the step becomes a partitioned `@asset` with
  the **folder-as-asset** contract (data_version = folder digest). Scope
  is `library × step`, branch as partition. Solves incremental + record
  + cross-library. This is the default target.
- **Tier 2 (per-step, not necessarily Dagster)** — per-PVT / per-cell
  fan-out that stays *inside* the step (per-leaf `.done` files,
  leaf-level deps). Tier 1 sees only the step's output folder + digest.
  Choose Tier 2 only when per-PVT control is needed AND a script
  refactor is in scope.

**Cardinality math first.** Before choosing a partition shape, write the
leaf count in the journal: `branches × steps × cells × PVTs × …`. The
math drives the tier boundary and the `PartitionsDefinition` choice
(Static / Dynamic / Multi; remember 1.13.3 `MultiPartitionsDefinition`
max 2 axes — collapse extra axes into composite keys, see lesson 09).

---

## Step 3 — Decompose into increments (the granularity ladder)

An **increment** is the smallest unit that can be **built, verified, and
smoke-run independently**. Prefer the smallest rung that is independently
verifiable. Rungs, smallest first:

1. **L0 config extraction** — take a partitionable source (a hardcoded
   per-PVT / per-cell list, or N near-duplicate source files) and
   express it as a **settings/config file + a generator** that writes
   the per-leaf files, instead of copying them. Maps to scale-lib
   `config/*.yaml` → `pipelines/factory.py`. *This is the user's
   explicit pain-point sub-task — prefer it whenever a step has
   `pvt_partitionable` or `cell_partitionable` sources.*
2. **L1 spec / rules** — pure-data dep facts (no Dagster import):
   `spec/`, `rules/` (`DepRule`).
3. **L2 registry** — compose rules → `DepEdge` (no Dagster import).
4. **L3 translator** — `PartitionRule` → `StaticPartitionMapping`
   (first Dagster import).
5. **L4 asset / factory** — one step → one partitioned `@asset` (or the
   factory that emits it); one runner wrap (plain subprocess vs Pipes).

Each increment is one row in `_plan.yaml` per `_schema/increment.yaml`:

```yaml
- id: a3                       # short stable id
  title: extract PVT list to config + generator
  target_layer: 0             # 0..4 (the rung above)
  step: liberate_run          # which flow step it serves (or '-')
  converts: "$FLOW_SRC/scripts/char/pvt_list.pl:12-40"   # source it replaces
  dagster_api: []             # public symbols it will use (filled at build; verify checks them)
  accept: "python -m _smoke"  # the command verify runs; exit 0 = pass
  depends_on: [a1, a2]        # increment ids (topo order)
  status: planned             # planned|building|built|verified|blocked|done
  note: ""
```

Rules for good increments:
- **Independently verifiable.** If you can't write a one-line `accept`
  command that proves it, split it smaller.
- **One source → one increment.** Don't bundle "convert all 7 steps" —
  that's the half-done / forget-the-rest failure mode.
- **Name the source.** Every increment cites the `$FLOW_SRC` path it
  converts, so a later tick can't blind-copy it by accident.

---

## Step 4 — Order topologically

Set `depends_on` so increments build in dependency order: layers
0→1→2→3→4, and within assets, upstream→downstream (cell_list → lpe →
corner_setup → … → signoff). The build loop always picks the
lowest-id `planned` increment whose `depends_on` are all `done`.

---

## Step 5 — Write the handoff and stop

Per the Wake SOP Step 4: write `_plan.yaml`, append `_operations.log`,
update `STATUS.md` (`next_action` = `build <first ready increment>`),
journal the model + cardinality math. The `plan` tick produces NO code.

---

## Re-evaluation (called by the `reflect` tick, user ask "定期重新評估")

When re-running on an existing ledger, do NOT discard it. Instead:

1. Mark `done` increments that the repo no longer needs as `obsolete`
   (don't delete — history matters; note in `_operations.log`).
2. Re-derive missing increments if the flow model changed (new step,
   new partition dim, new partitionable source).
3. Re-prioritize: `blocked` increments with a now-answered open question
   move back to `planned`.
4. If a pattern suggests the charter is wrong (a whole sub-area is
   off-target, or success criteria are unreachable), file ONE proposal
   to `flow-model/_open_questions.yaml` under `charter_proposals:` — the
   user hand-merges into `CONVERSION.md`. Never edit `CONVERSION.md`
   yourself.
