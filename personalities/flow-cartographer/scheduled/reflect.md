---
# flow-cartographer scheduled-task declaration. Register via the
# scheduling skill (.opencode/skills/scheduling/SKILL.md).
name: reflect
slug: am-flow-cartographer-reflect
description: Weekly reflect tick — meta-learn recurring failures, re-eval ledger, propose.
cron: "0 5 * * 0"                  # weekly Sunday 05:00. Adjust TZ.
timeout_seconds: 3600              # does more work; allow 60 min
permission_mode: allow             # writes _inbox proposals + re-evaluates the ledger + commits; default `deny` would block it
---
Run the weekly reflect tick: invoke `/wake flow-cartographer reflect`.

Follow `personalities/flow-cartographer/skills/wake/SKILL.md` — pre-flight
(Box 4/5 n/a, no live Dagster), read the handoff, then route to
`skills/reflect-loop/SKILL.md`. It scans recent verify findings for recurring
failure modes (> 2 of a type → file ONE improvement proposal to
`memory/lessons_learned/_inbox/`), re-evaluates the ledger (unblock / prune /
re-derive), proposes CONVERSION.md amendments (never auto-applies), writes a
digest, and runs the termination check. It writes NO code. Then write the
handoff; do not push.
