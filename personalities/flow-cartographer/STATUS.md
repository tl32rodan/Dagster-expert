<!-- all-might generated -->
---
allmight_status: v1
last_activity: 2026-05-26T00:00:00+00:00
active_increment: none
next_action: "fill CONVERSION.md (charter), then run /wake flow-cartographer plan"
---
# flow-cartographer — Status (the handoff baton)

This file is the handoff. **Read it first, write it last, every wake.**
A fresh agent with no memory of prior context resumes from this file +
`flow-model/_plan.yaml`. Keep it short and current.

## next_action

> See the `next_action:` field in the frontmatter above — the single
> most useful thing the next wake should do. The Wake SOP reads it at
> Step 2 and rewrites it at Step 4.

## Active focus

Convert a given execution flow into Dagster 1.13.3, one verified
increment at a time, on a schedule (plan → build → verify → reflect),
until `CONVERSION.md` success criteria are met. No flow charter is
filled yet — this personality was just evolved from `dagster-ap-auditor`.

## active_increment

none (no `CONVERSION.md` charter yet)

## recent_ticks

<!-- FIFO, max 5. Each tick appends one entry; drop the oldest. -->
- date: 2026-05-26
  tick: plan
  increment: "-"
  outcome: noop
  note: "personality scaffolded; awaiting CONVERSION.md + first plan tick"

## open_threads

- **CONVERSION.md not filled.** The charter (which flow = `$FLOW_SRC`,
  partition strategy, constraints, success criteria) must be filled
  before any plan/build tick does real work.
- **Schedule not wired on this box.** `scheduled/*.md` are declared but
  not yet registered with a runtime (opencode-scheduler / cron). See
  `.opencode/skills/scheduling/SKILL.md`.
- **Reflect threshold** for filing a recurring-failure proposal is
  currently "> 2 of the same finding type"; tune after first real runs.
