<!-- allmight_l1_cap=4096 -->
<!--
  L1 (MEMORY.md) is **portable-only** memory: what is true and useful no
  matter which corpus you work on. Keep it tight; over-cap triggers a
  passive nudge, not auto-eviction.

  Scope test: "still relevant in any workspace?" If no → not L1.

  Everything else belongs elsewhere:
  - Corpus-specific knowledge → memory/understanding/<workspace>.md
  - Open TODOs / session continuity → memory/<kind>/<workspace>.md
  - Searchable history → memory/journal/<workspace>/
-->

# Project Memory

> **Default personality**: dagster-expert
> **Active personality**: dagster-expert

## Project Map

| Personality | Capabilities | Scope | Active focus |
|-------------|--------------|-------|--------------|
| dagster-expert | database, memory | Dagster 1.13.3 air-gap: TEACHER (11 lessons) + OPERATOR (bootstrap/diagnose) + LIBRARIAN (offline API lookup); merged from `dagster-operator` + `dagster-tutor` + `dagster-librarian` v3 bundles | *(set by agent via /remember; see `personalities/dagster-expert/STATUS.md`)* |

See each personality's `STATUS.md` for richer rolling state
(active focus, recent topics, open threads). The "Active focus"
column above is a one-line summary; STATUS.md has the long form.
See `memory/understanding/<workspace>.md` for detailed per-corpus
knowledge.

## User Preferences

- Shell is **tcsh** (use `setenv` syntax first in any shell example;
  show `export` for bash in parentheses).
- Air-gapped TSMC workstation: no internet at runtime, no public PyPI /
  Docker registries, no Dagster+ / Cloud, no `uv` / `dg` / k8s.
- Primary agent runtime is **Minimax M2.5** (or similar less-capable
  agents). Personality instructions are mechanical / checklist-driven
  rather than judgment-based. See
  `personalities/dagster-expert/PRE_FLIGHT_CHECKLIST.md`.
- **Graph-theory terminology over domain-specific labels** in
  abstractions: use `parent_of` / `is_root` / `ancestors_of` rather
  than `corner_of` / `is_corner`. The branch literally named ``corner``
  keeps its name; the role is ``root``.
- **Cardinality math first** when scaling: enumerate the total leaf count
  (branches × steps × cells × PVTs × ...) before committing to a
  partition / asset design. The math drives the tier-boundary, not the
  framework's API surface.

## Active Goals

- Walk through Dagster 1.13.3 lessons end-to-end without skipping
  per-lesson DAGSTER_HOME isolation or the librarian-consult-before-code
  hard rule.

## Key Facts

- Dagster version is pinned at **1.13.3** across all examples,
  cheatsheets, and lesson code.
- `DAGSTER_HOME` is **per-lesson** in TEACHER mode
  (`~/.dagster-tutor/<NN-topic>`) and **fixed** in OPERATOR mode
  (`/var/lib/dagster` for prod, `~/.dagster` for dev).
- SMAK vector indices (`personalities/*/database/*/store/`,
  `personalities/*/memory/store/`) are gitignored and rebuilt by
  `/ingest`.

## Lessons learned — designing personalities for less-capable agents

Captured 2026-05-11 during the build of `dagster-expert`. The goal
was a single personality that **Minimax M2.5 / Kimi K2.5** can drive
without skipping pre-conditions. Three Haiku-as-Minimax dry runs
(TEACHER / OPERATOR / LIBRARIAN modes) all passed 8/8 criteria after
applying the rules below.

### 1. Mechanical triggers over judgment
A rule like "always consult librarian first" gets skipped by
less-capable agents because they don't know when to apply it. Rewrite
the same rule as a mechanical regex trigger:
> Before writing any line matching `^from dagster import`, run the
> mechanical lookup sequence. 0 results ⇒ REFUSE.

### 2. Pre-flight as a standalone file, not a paragraph
A multi-paragraph "always do X first" gets paraphrased away. A
standalone `PRE_FLIGHT_CHECKLIST.md` with 7 numbered boxes the agent
must tick **out loud** does not.

### 3. Mode decision tree at the top of ROLE.md
The first action on any request is matching trigger words against a
table; the first match wins; the mode is declared out loud and
carried through the conversation. No judgment, just match.

### 4. Shell-aware command blocks
The user's session is **tcsh**. Always show `setenv VAR value` first
and `export VAR=value` in parentheses. Less-capable agents will
otherwise copy the bash-only `export …` and the user will paste it
into tcsh where it fails silently.

### 5. Absolute paths only — no `cd` chains
Every `dagster` command takes `-w /abs/path/to/workspace.yaml`. `cd`
chains break when the agent reasons about pwd state across turns.

### 6. Verify-after-each-step
Every command is paired with its verify command and the expected
output. The agent reads the output before continuing.

### 7. Refusal as a feature
Hard rules become refusals with the exact remediation, not best-effort
warnings. "Refuse to launch dagster if `echo $DAGSTER_HOME` is empty;
the remediation is `setenv DAGSTER_HOME …`".

### 8. Visible state checkpoints
At the start of any multi-step task, print `echo $DAGSTER_HOME`,
`which dagster`, `dagster --version`. The human + agent can both see
state instead of inferring it.

### Bug found during validation that informed the final design
- **Per-lesson DAGSTER_HOME isolation.** Initial design used one shared
  `~/.dagster-tutor` for all 11 lessons; storage and runs would pollute
  across lessons. The user flagged this; final design pins
  `~/.dagster-tutor/<NN-topic>` per lesson, with a reminder baked into
  ROLE.md TEACHER mode, `PRE_FLIGHT_CHECKLIST.md` Box 2, and
  `learn/ENV_SETUP.md` Step 1. The agent must re-set `DAGSTER_HOME`
  every time the learner switches lessons.
