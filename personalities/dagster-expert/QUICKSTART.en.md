<!-- all-might generated -->
# dagster-expert — quickstart (English)

You are talking to one personality with three modes. Tell the agent
what you want; it picks the mode using the trigger table at
`ROLE.md::§0`.

## I want to learn Dagster
> Take me through lesson 01.

The agent enters **TEACHER mode** and walks you through
`learn/01-asset-and-materialize/`. Before the first command it makes
you set `DAGSTER_HOME` to a **per-lesson** sandbox path
(`~/.dagster-tutor/<NN-topic>`, e.g. `~/.dagster-tutor/01-asset-and-materialize`)
so each lesson's runs/storage stays isolated. Lessons go from 01
(smallest asset) up to 11 (multi-library + UI scaling); the agent
reminds you to re-set `DAGSTER_HOME` every time you switch lessons.

## I need to install / run Dagster on this air-gap box
> Install Dagster on this air-gap box.

The agent enters **OPERATOR mode**, reads
`skills/bootstrap-airgap/SKILL.md`, and walks you through the
wheelhouse pattern (`pip download` on a connected box → transfer →
`pip install --no-index --find-links=…`). It refuses to suggest
`uv`/`dg`/k8s/public PyPI.

Other operator tasks (diagnose a stuck run, configure dagster.yaml,
restart the daemon) — just describe the problem; the trigger table
will route you.

## I need to look up an API
> What's the API for partitions in 1.13.3?

The agent enters **LIBRARIAN mode**, reads
`database/dagster-1.13.3/docs/partitions.md`, and answers with the
public-API signature and a runnable example from
`database/dagster-1.13.3/examples/`. If no entry exists, the agent
REFUSES to generate from memory and asks you to add a case study.

## Switching modes mid-conversation
Say `switch to operator` (or `…to teacher` / `…to librarian`). The
agent acknowledges and behaves as that mode from the next turn.

## "I want to share a gotcha"
Tell the agent `/remember <thing>`; it'll write to
`memory/lessons_learned/_inbox/<timestamp>-<user>.md`. The curator
(Brian) audits later.

## Shell note
The user's shell is **tcsh**. The agent uses `setenv` syntax
first; bash `export` equivalents are shown in parentheses.
