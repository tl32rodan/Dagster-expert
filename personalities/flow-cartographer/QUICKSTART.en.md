<!-- all-might generated -->
# flow-cartographer — quickstart (English)

You are talking to **flow-cartographer**: give it an execution flow and
it converts that flow into Dagster 1.13.3, one verified increment at a
time, on a schedule (plan → build → verify → reflect), until done. It is
built for less-capable agents on air-gap boxes, so it is mechanical and
state-on-disk: it never relies on remembering the conversation.

## One-time setup (the charter)

1. Point it at the flow:
   ```
   setenv FLOW_SRC /abs/path/to/flow      # tcsh
   export FLOW_SRC=/abs/path/to/flow      # bash
   echo $FLOW_SRC && ls $FLOW_SRC | head  # verify
   ```
2. Fill the charter `personalities/flow-cartographer/CONVERSION.md`:
   the flow name, goal, steps in scope, **which sources are PVT-/
   cell-partitionable** (these become config + generator, not copies),
   constraints, and success criteria. Replace every `[PLACEHOLDER]`.
   The first real target is the **real-char pipeline** (production form
   of `dagster-expert/learn/09-real-flow/`).

That's it. From here the loop reads the charter; you don't hand-write
tasks.

## Running it

The loop runs as four scheduled ticks (or run them by hand to dry-run):

| Command | Does |
|---|---|
| `/wake flow-cartographer plan` | model the flow + build the increment ledger |
| `/wake flow-cartographer build` | convert ONE increment to Dagster |
| `/wake flow-cartographer verify` | self-check that increment → done or blocked |
| `/wake flow-cartographer reflect` | weekly: learn from recurring failures + re-plan |

Every tick reads the handoff first (`STATUS.md` + `flow-model/_plan.yaml`)
so it always knows where the last tick left off — that is how it never
"forgets where it was". To schedule them unattended, see
`.opencode/skills/scheduling/SKILL.md` (cron on the air-gap box).

## Where to look

- **What it's doing / where it is**: `STATUS.md` (the `next_action`
  baton) and `flow-model/_plan.yaml` (the increment ledger).
- **What it couldn't decide**: `flow-model/_open_questions.yaml` — it
  parks questions for you instead of guessing.
- **What it did**: `flow-model/_operations.log` + the journal under
  `memory/journal/<flow-name>/`.
- **Why a step keeps failing**: `reflect` files a proposal in
  `memory/lessons_learned/_inbox/` for you to review.

## Guarantees (the guardrails)

- It uses only **public Dagster 1.13.3 API** (checked against the
  sibling corpus); no private imports.
- It **converts, never blind-copies**: a copied-in source file is a
  failure.
- An increment isn't "done" until its **smoke command actually runs**.

## Shell + sibling

tcsh-first (`setenv` shown first, `export` in parens). For Dagster
*teaching / operating / API lookup* (not conversion), say "switch to
dagster-expert".
