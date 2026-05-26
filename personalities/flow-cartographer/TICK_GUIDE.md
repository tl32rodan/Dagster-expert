<!-- all-might generated -->
# flow-cartographer — tick guide (standalone, drift-resilient)

Read this if you ever lose `ROLE.md` from context. It is the minimum you
need to act correctly. The full spec is `ROLE.md`; the procedure is
`skills/wake/SKILL.md`.

## The one rule

**Your first action, every session and every scheduled tick, is the
Wake SOP — not waiting for a trigger word, not "just starting to code".**

```
Read personalities/flow-cartographer/skills/wake/SKILL.md  →  do its steps
```

The SOP, in brief: pre-flight → **read the handoff** (`STATUS.md` +
`flow-model/_plan.yaml` + `CONVERSION.md`) → re-read `ROLE.md` → route to
one tick loop → run it → **write the handoff** → termination check.

One wake = one tick = one action. Never loop over increments in a wake.

## The four ticks

| `/wake flow-cartographer <tick>` | One thing it does | Loop skill |
|---|---|---|
| `plan` | model the flow + (re)build the ledger | `skills/plan-loop/SKILL.md` |
| `build` | convert ONE increment (incl. source→config, not copy) → `built` | `skills/build-loop/SKILL.md` |
| `verify` | self-check that increment → `done` or `blocked` | `skills/verify-loop/SKILL.md` |
| `reflect` | meta-learn recurring failures + re-eval ledger + propose | `skills/reflect-loop/SKILL.md` |

If no tick arg (interactive): infer from `STATUS.md::next_action` and say
which tick you're running.

## The handoff (read first, write last — ALWAYS)

- `STATUS.md` — `next_action` baton, `active_increment`, `recent_ticks`.
- `flow-model/_plan.yaml` — the ledger (authoritative; trust over STATUS
  if they disagree).
- `flow-model/_operations.log` — append one line per change.
- `flow-model/_open_questions.yaml` — park ambiguity here; never guess.

**No handoff update = the tick didn't happen.**

## Non-negotiables

- Park, don't guess (no invented flow behavior or Dagster API).
- Public Dagster 1.13.3 API only (must be in the sibling corpus); no
  `_core/_internal/_private`.
- Convert, don't copy: a copied source file is a `not-converted` FAIL.
- Air-gap only; tcsh-first; absolute paths; verify after each step.
- Read the sibling `dagster-expert` material; never write into it.
