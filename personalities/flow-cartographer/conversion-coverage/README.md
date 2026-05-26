<!-- all-might generated -->
# conversion-coverage/ — the 5 behaviors a conversion must preserve

These five checklists were the old `dagster-ap-auditor` parity audits.
They are repurposed: instead of a standalone PASS/REJECT product, they
are now a **coverage checklist the `verify` tick consults** (check 6 in
`skills/verify-loop/SKILL.md`). When an increment converts a flow step,
the verify tick asks: does the conversion preserve this behavior, or is
the gap explicitly parked as an open question?

| # | Behavior a conversion must cover | File |
|---|---|---|
| 01 | State management (run/asset/partition state, storage) | `01-state-management.md` |
| 02 | Stop & rerun (cancel/kill/resume/checkpoint) | `02-stop-and-rerun.md` |
| 03 | Job scheduling (schedules, sensors, daemon, backfills) | `03-job-scheduling.md` |
| 04 | Dependency definition (`@asset` deps, MultiPartitions, Style A/B) | `04-dependency-definition.md` |
| 05 | Logs & env status (event log, compute logs, pipes, daemon) | `05-logs-and-env-status.md` |

## How the verify tick reads these

These are a **coverage checklist**, not a standalone PASS/REJECT audit.
For the step an increment serves, open the matching `0N-*.md`, run its
criteria as a checklist, and for each: confirm the Dagster conversion
preserves the behavior (cite the public API) and mark it **covered**,
OR record the **gap** in `flow-model/_open_questions.yaml`. An
unaddressed gap → verify FAIL `coverage-gap`; a gap explicitly parked as
an open question is acceptable, not a hard reject.

New criteria rows come from a curator commit, never auto-added mid-tick.

## What carried over unchanged

- **Public API only.** Every Dagster mapping cites a symbol in
  `personalities/dagster-expert/database/dagster-1.13.3/docs/`. No
  `dagster._core/_internal/_private`.
- **No verbal-only claims.** "should work" → restate as "<flow behavior
  X> maps to <Dagster API Y> because <evidence Z>".
- **Cardinality-math-first** for any partition/storage choice (C6 in 01).
