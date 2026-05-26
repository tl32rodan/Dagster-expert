---
name: reflect-loop
description: >
  Weekly meta-learning + ledger re-evaluation. Detects recurring
  failure modes across recent findings and files improvement proposals;
  re-evaluates the conversion ledger; proposes CONVERSION.md amendments.
  Runs on the `reflect` tick. Never edits code; never auto-applies a
  proposal.
---

# reflect-loop — observe the loop, propose fixes

This is how the system gets less dumb over time without anyone watching
each tick. You look at the last several ticks as a whole, find patterns,
and write proposals for a human (the curator) to merge. You change
NOTHING that you can't undo with a `git revert`: no code, no
`CONVERSION.md` edits, no `_reviewed/` writes.

## Step 1 — Recurring-failure scan (the meta-learning step)

Read the most recent findings in `flow-model/_open_questions.yaml`
(`findings:`) and the last ~10 `_operations.log` lines.

Count findings by `type` (`invented-api`, `not-converted`,
`smoke-failed`, `coverage-gap`, `private-import`, `uncited`).

**If any single `type` appears > 2 times across recent findings**, the
loop has a systemic weakness, not a one-off bug. File ONE proposal to
`memory/lessons_learned/_inbox/<ISO>-<unix_user>.md`:

```markdown
# reflect: recurring <type> (<n> times since <date>)

## Pattern
<which increments, what the failures share>

## Hypothesis
<why a weak model keeps hitting this — e.g. "build-loop doesn't force a
lookup-api call before writing imports", or "the source→config rule is
buried, so the model copies instead">

## Proposed fix (curator decides)
<one concrete change to a SKILL.md / ROLE.md / CONVERSION.md / a
conversion-coverage row that would prevent the recurrence>
```

Example: three `not-converted` findings ⇒ propose strengthening
`build-loop/SKILL.md` Step 3's "do NOT copy" rule, or adding an L0
config-extraction increment the planner missed. This is the antidote to
"the agent keeps blind-copying / forgetting to convert".

## Step 2 — Re-evaluate the ledger

Follow `skills/plan-loop/SKILL.md` § "Re-evaluation": unblock
now-answered increments, mark obsolete ones, re-derive missing
increments if the flow model changed, re-prioritize. Append every
structural change to `_operations.log`.

## Step 3 — Charter-amendment proposals (never auto-apply)

If you observe that the charter itself is off — a whole step is out of
scope, a success criterion is unreachable, or an emergent partition
dimension wasn't in the original plan — file at most ONE proposal under
`flow-model/_open_questions.yaml::charter_proposals:`:

```yaml
charter_proposals:
  - date: YYYY-MM-DD
    observation: "<what pattern suggests the charter is wrong>"
    suggested_edit: "<one concrete CONVERSION.md change>"
```

The user reads and hand-merges accepted proposals into `CONVERSION.md`.
Direction stays human-owned.

## Step 4 — Digest

Write `flow-model/digest/<YYYY-MM-DD>.md`, human-shaped and short:
- progress: `done` / `total` increments, % by layer
- 1–3 recurring patterns (or "none")
- open blockers + parked questions
- 1 health line (e.g. "smoke green on all done increments; 2 blocked on
  open questions for the user")

## Step 5 — Termination check + handoff

Run the Wake SOP Step 5 termination check (are `CONVERSION.md` success
criteria met?). Then write the handoff (Step 4): `_operations.log`
(`<ts> reflect - <proposals-filed>`), `STATUS.md` (`next_action`), and a
journal entry. The `reflect` tick produces proposals + a digest, never
code.
