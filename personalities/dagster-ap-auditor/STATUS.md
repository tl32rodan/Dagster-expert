<!-- all-might generated -->
---
allmight_status: v1
last_activity: 2026-05-13T00:00:00+00:00
---
# dagster-ap-auditor — Status

## Active focus

Phase 1 of the Dagster 1.13.3 ↔ AP compatibility migration. Auditing
five behavioral dimensions for parity:

1. State management (run / asset / partition state, storage backends)
2. Stop & rerun (cancel / kill / resume / checkpoint)
3. Job scheduling (schedules, sensors, daemon, partitions, backfills)
4. Dependency definition (@asset deps=, AssetIn, MultiPartitions, Style A/B)
5. Logs & env status (event log, compute logs, pipes, instance migrate, daemon liveness)

Modes ready: CHARTER (architecture/migration plan review), CODE (TDD +
clean code review), SMOKE (CLI + GraphQL conformance execution).

## Recent topics

- 2026-05-13 personality scaffold created (ROLE.md, MODE_DECISION_TREE,
  PRE_FLIGHT_CHECKLIST, manifest, 5 audit checklists, standards/,
  smoke/, memory/). Sibling of dagster-expert; reads
  database/dagster-1.13.3 + learn/ without duplicating.

## Open threads

- AP source path: not yet set on this workstation. Every session must
  begin with the PRE_FLIGHT Box 3 prompt for the user to
  `setenv AP_SRC /abs/path/to/ap`.
- audits/0N-….md checklists currently carry the uniform template +
  starter criteria. As real AP behaviors are discovered, criteria
  rows should be added (curator-edit, not auto-generated).
- smoke/cli-conformance.md and smoke/graphql-conformance.md tables
  begin with skeleton rows. Real AP CLI / GraphQL contract rows are
  added as audits surface them.
- No SMAK index over `$AP_SRC` (by design — AP is path-deployed, no
  git, no version pin). All AP lookups are via Read/grep on
  `$AP_SRC`.
- memory/journal/<workspace>/ scope name not yet decided. Default is
  the AP module under audit (e.g. "ap-scheduler", "ap-runtime").
