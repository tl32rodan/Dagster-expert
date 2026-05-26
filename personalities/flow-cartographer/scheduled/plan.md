---
# flow-cartographer scheduled-task declaration. Register with the chosen
# runtime via the scheduling skill (.opencode/skills/scheduling/SKILL.md):
# opencode-scheduler schedule_job (slug below) on OpenCode, or external
# cron on the air-gap box. All-Might does not auto-apply these yet.
name: plan
slug: am-flow-cartographer-plan
description: Weekly (re)model the flow + rebuild the conversion ledger.
cron: "0 18 * * 1"                 # weekly Mon 18:00; also run on demand after CONVERSION.md edits. Adjust TZ.
timeout_seconds: 1800
permission_mode: allow             # the loop must Read/Write flow-model/* + commit; scheduler default `deny` would block it
---
Run one plan tick: invoke `/wake flow-cartographer plan`.

Follow `personalities/flow-cartographer/skills/wake/SKILL.md` — pre-flight,
read the handoff, re-read ROLE.md, then route to
`skills/plan-loop/SKILL.md`. It models `$FLOW_SRC` into
`flow-model/steps/*.yaml` and (re)builds the ledger `flow-model/_plan.yaml`;
it writes NO code. Then write the handoff (STATUS.md + _operations.log +
journal). Commit locally; do not push.
