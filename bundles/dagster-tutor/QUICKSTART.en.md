# QUICKSTART — `dagster-tutor` (English)

You want to learn Dagster. This personality has 8 progressive
lessons. Pick where to start.

## "I've never touched Dagster"

Start at lesson 01 and go in order. Each takes 30–90 minutes.

Tell the agent (no CLI subcommand — switching is verbal):
- "switch to dagster-tutor" — the agent updates `MEMORY.md`'s `> **Active personality**:` line
- Then "let's do 01-asset-and-materialize"

The agent will walk you through README → code → run → exercise →
recap. ~6–8 hours total to cover all 8 lessons.

## "I know X already, what's next?"

| You know | Start at |
|---|---|
| `@asset` and Materialize button | 02-deps-and-lineage |
| Asset deps and lineage | 03-partitions |
| Static partitions | 04-runconfig (or skip to 06 if you're focused on ops) |
| Run config and failures basics | 06-interrupt-rerun (recovery semantics) |
| Multi-code-location workspaces | 07 (Day-7 federation bug case) |
| All of the above | 08-complex-deps (sparse-matrix DAGs) |

## "I have a specific question"

Don't pick a lesson — just ask. The agent will route you:

- "How do I parameterize a run?" → lesson 04
- "What happens when I cancel a run?" → lesson 06a
- "Why does my dependency look wrong across libraries?" → lesson 07

If your question doesn't map to any lesson, the agent answers
from general knowledge with a confidence tag and (optionally)
files a `/remember` case study.

## "I want to do this myself, no walkthrough"

Each lesson is self-contained:

```bash
cd personalities/dagster-tutor/learn/<NN-topic>/
# Read README.md
dagster dev -m <module-name>     # most lessons; see README
# or
dagster dev -w workspace.yaml    # lessons 07, 08
```

Open http://127.0.0.1:3000.

## What this personality WON'T do

- Set up Dagster on your air-gap host (that's
  `dagster-operator`'s `bootstrap-airgap`)
- Diagnose production outages (`dagster-operator`'s `diagnose-*`
  skills)
- Tell you to use `uv`, `dg`, `pipx`, k8s, Dagster+ — out of scope

If you ask, the agent will say "switch to `dagster-operator`".
Do that by telling the agent: **"switch to dagster-operator"**.
It updates `MEMORY.md`'s active personality callout (one line).
There is no CLI subcommand for switching.

## Lesson catalog (one line each)

| # | Topic | What you take away |
|---|---|---|
| 01 | Asset & materialize | The smallest possible Dagster loop |
| 02 | Dependencies & lineage | What makes Dagster ≠ a job runner |
| 03 | Partitions | The "for-loop" of Dagster |
| 04 | Run config | Parameterizing a run from the UI |
| 05 | Failures, retries | Asset-level failure semantics |
| 06 | Interrupt + rerun | Run-level state machine recovery |
| 07 | Cross-location | Multi-team / multi-library DAGs (incl. Day-7 bug) |
| 08 | Complex deps | Sparse-matrix DAGs (route A vs B) |

## Versions and prerequisites

- Dagster 1.13.3 (pinned across all lessons)
- Python 3.10+
- A venv with `dagster==1.13.3` and `dagster-webserver==1.13.3`
- For air-gap install: switch to `dagster-operator`,
  `bootstrap-airgap` skill

## Want to give feedback?

If a lesson tripped you up or had a wrong example, tell the
agent. It'll file a case study to
`memory/lessons_learned/_inbox/`. Brian (the curator) audits and
either updates the lesson or moves it to `_reviewed/` as a kept
record.

Don't edit the lesson README directly — curator-only.

## Ready?

Pick a lesson:
- New: "let's do 01"
- Resuming: "let's do 06"
- Specific question: just ask it

The agent will follow `skills/walkthrough-lesson/SKILL.md` —
README → code → run → observe → exercise → recap.
