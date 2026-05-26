---
# flow-cartographer scheduled-task declaration. Register via the
# scheduling skill (.opencode/skills/scheduling/SKILL.md).
name: verify
slug: am-flow-cartographer-verify
description: Off-peak verify tick — self-check the just-built increment (verify-gate).
cron: "30 20-23,0-7 * * *"         # hourly off-peak at :30, 30 min AFTER build. Adjust TZ.
timeout_seconds: 1800
permission_mode: allow             # the loop runs smoke (Bash) + commits a passing increment; default `deny` would block it
---
Run one verify tick: invoke `/wake flow-cartographer verify`.

Follow `personalities/flow-cartographer/skills/wake/SKILL.md` — pre-flight,
read the handoff, then route to `skills/verify-loop/SKILL.md`. It gates the
`built` increment with a different framing: API-exists-in-corpus, no private
imports, smoke actually runs, converted-not-copied, source cited, coverage
preserved. All pass → `done` + local commit; any fail → `blocked` + a finding
in `flow-model/_open_questions.yaml` (the next build tick fixes it). Then
write the handoff; do not push.
