# Dagster-expert

Air-gapped Dagster 1.13.3 agent project — one All-Might v4 personality
with three internal modes (TEACHER, OPERATOR, LIBRARIAN), designed for
less-capable agents (Minimax M2.5, Kimi K2.5) running on TSMC air-gap
workstations.

## Personalities

| Name | Capabilities | Source of truth |
|---|---|---|
| dagster-expert | database, memory | `personalities/dagster-expert/ROLE.md` |

## Where the workflow & instructions live

**Always `personalities/dagster-expert/ROLE.md`.** The `role-load` hook
re-injects that file at every chat turn, so the agent sees it directly.
`ROLE.md` contains:

- **Mode Decision Tree** at top (TEACHER / OPERATOR / LIBRARIAN trigger
  table; mechanical, not judgment)
- **Mandatory pre-flight** pointer to
  `personalities/dagster-expert/PRE_FLIGHT_CHECKLIST.md` (7 boxes ticked
  out loud at session start)
- Per-mode workflows, skill tables, hard rules, refusal patterns
- Shell-aware command syntax (tcsh-first; bash equivalents in parentheses)
- Per-lesson `DAGSTER_HOME` isolation requirement

Companion files alongside `ROLE.md`:
- `MODE_DECISION_TREE.md` — standalone copy resilient to context drops
- `manifest.yaml` — capabilities + `derived_from` lineage (3 v3 sources)
- `QUICKSTART.{en,zh}.md` — bilingual user-facing intro
- `learn/ENV_SETUP.md` — lesson env setup with per-lesson DAGSTER_HOME
- `memory/lessons_learned/_inbox/` — case study write target

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
