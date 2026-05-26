---
# flow-cartographer scheduled-task declaration. Register via the
# scheduling skill (.opencode/skills/scheduling/SKILL.md).
name: build
slug: am-flow-cartographer-build
description: Off-peak build tick — convert exactly one ledger increment to Dagster.
cron: "0 20-23,0-7 * * *"          # hourly off-peak at :00. Adjust cron + box TZ per deployment.
timeout_seconds: 1800
permission_mode: allow             # the loop must Edit/Write code + commit; scheduler default `deny` would block it
---
Run one build tick: invoke `/wake flow-cartographer build`.

Follow `personalities/flow-cartographer/skills/wake/SKILL.md` — pre-flight
(incl. $DAGSTER_HOME + venv + dagster 1.13.3), read the handoff, then route
to `skills/build-loop/SKILL.md`. It converts ONE increment (the lowest-id
`planned` whose deps are `done`), including the source→config transform for
partitionable sources — never a blind copy. It marks the increment `built`
(NOT `done`; the :30 verify tick promotes it). Then write the handoff and
commit the increment locally; do not push.
