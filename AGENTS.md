# Dagster-expert

Air-gapped Dagster 1.13.3 agent project — All-Might v4 personalities
designed for less-capable agents (Minimax M2.5, Kimi K2.5) running on
TSMC air-gap workstations. Two personalities co-exist:

- `dagster-expert` — daily driver. Three internal modes: **TEACHER**
  (11 lessons), **OPERATOR** (bootstrap / diagnose), **LIBRARIAN**
  (offline API lookup).
- `dagster-ap-auditor` — strict acceptance gatekeeper for the
  Phase-1 Dagster 1.13.3 ↔ AP compatibility migration. Three internal
  modes: **CHARTER** (architecture / migration plan review),
  **CODE** (TDD + clean-code review), **SMOKE** (CLI + GraphQL
  conformance execution). Reads `dagster-expert`'s
  `database/dagster-1.13.3/` and `learn/` material; never duplicates.

## Personalities

| Name | Capabilities | Source of truth |
|---|---|---|
| dagster-expert | database, memory | `personalities/dagster-expert/ROLE.md` |
| dagster-ap-auditor | memory | `personalities/dagster-ap-auditor/ROLE.md` |

## Where the workflow & instructions live

**For Dagster teaching / operating / API lookup:**
`personalities/dagster-expert/ROLE.md`. Mode Decision Tree at top
(TEACHER / OPERATOR / LIBRARIAN), pre-flight pointer to its
`PRE_FLIGHT_CHECKLIST.md` (7 boxes), per-mode workflows, hard rules,
tcsh-first shell syntax, per-lesson `DAGSTER_HOME` isolation.

**For Dagster ↔ AP compatibility acceptance:**
`personalities/dagster-ap-auditor/ROLE.md`. Mode Decision Tree at top
(CHARTER / CODE / SMOKE), pre-flight pointer to its
`PRE_FLIGHT_CHECKLIST.md` (8 boxes including `$AP_SRC`), per-mode
workflows with strict refusal patterns, binary PASS/REJECT verdicts.

The `role-load` hook injects ALL `personalities/*/ROLE.md` files at
every chat turn, so both ROLEs are in context simultaneously. The
trigger-word table inside each ROLE decides which personality answers
a given user message; if the user says "switch to <other>", that's
the explicit handover.

Companion files alongside each `ROLE.md`:
- `MODE_DECISION_TREE.md` — standalone copy resilient to context drops
- `PRE_FLIGHT_CHECKLIST.md` — mandatory session boxes
- `manifest.yaml` — capabilities + `derived_from` lineage
- `QUICKSTART.{en,zh}.md` — bilingual user-facing intro
- `memory/lessons_learned/_inbox/` — case study write target

`dagster-expert` adds: `learn/ENV_SETUP.md` (per-lesson
DAGSTER_HOME), `database/dagster-1.13.3/` (API corpus), `skills/`
(custom skills), `demo/` (production-shaped reference).

`dagster-ap-auditor` adds: `audits/0N-….md` (5 parity checklists),
`standards/` (TDD + clean-code rules + refusal templates),
`smoke/` (CLI + GraphQL conformance rows).

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
