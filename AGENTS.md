# Dagster-expert

Air-gapped Dagster 1.13.3 agent project — All-Might v4 personalities
designed for less-capable agents (Minimax M2.5, Kimi K2.5) running on
TSMC air-gap workstations. Two personalities co-exist:

- `dagster-expert` — daily driver. Three internal modes: **TEACHER**
  (11 lessons), **OPERATOR** (bootstrap / diagnose), **LIBRARIAN**
  (offline API lookup).
- `flow-cartographer` — given any execution flow (`$FLOW_SRC` +
  `CONVERSION.md`), runs a scheduled **plan → build → verify →
  reflect** loop that converts it to Dagster 1.13.3 one verified
  increment at a time, until the charter's success criteria are met.
  Its first action every session/tick is the Wake SOP (not a
  trigger-word match). Evolved from the retired `dagster-ap-auditor`
  acceptance gatekeeper, whose mechanical guardrails survive as the
  `verify` tick's self-check. Reads `dagster-expert`'s
  `database/dagster-1.13.3/`, `learn/`, and `demo/scale-lib/` material
  as ground truth; never duplicates it.

## Personalities

| Name | Capabilities | Source of truth |
|---|---|---|
| dagster-expert | database, memory | `personalities/dagster-expert/ROLE.md` |
| flow-cartographer | memory, schedule | `personalities/flow-cartographer/ROLE.md` |

## Where the workflow & instructions live

**For Dagster teaching / operating / API lookup:**
`personalities/dagster-expert/ROLE.md`. Mode Decision Tree at top
(TEACHER / OPERATOR / LIBRARIAN), pre-flight pointer to its
`PRE_FLIGHT_CHECKLIST.md` (7 boxes), per-mode workflows, hard rules,
tcsh-first shell syntax, per-lesson `DAGSTER_HOME` isolation.

**For converting an execution flow to Dagster:**
`personalities/flow-cartographer/ROLE.md`. The §0 Wake SOP is the first
action every session and every scheduled tick — read the handoff
(`STATUS.md` + `flow-model/_plan.yaml`), re-read ROLE, route to one tick
loop, run it, write the handoff. Driven by `CONVERSION.md` (the charter)
+ `$FLOW_SRC`, not by trigger words.

The `role-load` hook injects ALL `personalities/*/ROLE.md` files at
every chat turn, so both ROLEs are in context simultaneously.
`dagster-expert` answers teaching/operating/lookup questions by its
trigger table; `flow-cartographer` runs the conversion loop. If the
user says "switch to <other>", that's the explicit handover.

Companion files alongside each `ROLE.md`:
- standalone drift-resilient copy — `dagster-expert`'s
  `MODE_DECISION_TREE.md`; `flow-cartographer`'s `TICK_GUIDE.md`
- `PRE_FLIGHT_CHECKLIST.md` — mandatory session boxes
- `manifest.yaml` — capabilities + `derived_from` lineage
- `QUICKSTART.{en,zh}.md` — bilingual user-facing intro
- `memory/lessons_learned/_inbox/` — case study write target

`dagster-expert` adds: `learn/ENV_SETUP.md` (per-lesson
DAGSTER_HOME), `database/dagster-1.13.3/` (API corpus), `skills/`
(custom skills), `demo/` (production-shaped reference).

`flow-cartographer` adds: `CONVERSION.md` (the user-owned charter),
`flow-model/` (live conversion state: ledger `_plan.yaml`, step nodes,
`_operations.log`, `_open_questions.yaml`), `skills/{wake,plan-loop,
build-loop,verify-loop,reflect-loop}/` (the loop SOPs), `scheduled/`
(the four `am-flow-cartographer-<tick>` task declarations),
`conversion-coverage/0N-….md` (the 5 behavioral aspects a conversion
must cover — repurposed from the old audits), `standards/` + `smoke/`
(verify-tick inputs).

## Why this `AGENTS.md` is minimal (and hand-curated)

`/all-for-one` and `/one-for-all` skills bundle and merge `ROLE.md`, not
`AGENTS.md`. Keeping `AGENTS.md` as a thin pointer (no `<!-- all-might
generated -->` marker, intentionally hand-authored) makes:

- the personality cleanly bundleable via `/one-for-all`
- the workflow/instructions discoverable in exactly one place (`ROLE.md`)
- this file safe from auto-regeneration on `allmight init` re-runs (the
  framework only overwrites files carrying its marker)

If you ever do want the verbose auto-composed form back, run `allmight
init . --force` and re-add the personality; the framework will
regenerate `AGENTS.md` from `ROLE.md`.

## Project-wide capability docs

For `database` (knowledge graph: `/search`, `/enrich`, `/ingest`,
`/onboard`, `/sync`, `/one-for-all`, `/all-for-one`) and `memory`
(L1/L2/L3 + `/remember`, `/recall`, `/recover`) skills, see
`.opencode/skills/` and `.opencode/commands/`. Those are framework
globals shared across all personalities and not specific to
dagster-expert.
