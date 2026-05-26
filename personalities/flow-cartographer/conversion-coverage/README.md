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

## How the verify tick reads these (translation from the old framing)

The five dimension files still read in the old auditor's vocabulary.
Apply this mapping when you use them — do NOT treat them as a separate
audit:

| Old auditor term (in the 0N files) | Read it now as |
|---|---|
| `$AP_SRC` | `$FLOW_SRC` (the source flow being converted) |
| "the migration plan" | the current build increment |
| PASS / REJECT (binary verdict) | covered / gap (and a gap that is parked in `_open_questions.yaml` is acceptable, not a hard REJECT) |
| "curator adds criteria rows" | still true — new rows come from a curator commit, never auto-added mid-tick |

So for the step an increment serves, open the matching `0N-*.md`, run
its criteria as a checklist, and for each: confirm the Dagster
conversion preserves the behavior (cite the public API), OR record the
gap as an open question. An unaddressed gap → verify FAIL `coverage-gap`.

## What carried over unchanged

- **Public API only.** Every Dagster mapping cites a symbol in
  `personalities/dagster-expert/database/dagster-1.13.3/docs/`. No
  `dagster._core/_internal/_private`.
- **No verbal-only claims.** "should work" → restate as "<flow behavior
  X> maps to <Dagster API Y> because <evidence Z>".
- **Cardinality-math-first** for any partition/storage choice (C6 in 01).

> Cleanup note (open thread): the 0N-*.md bodies still use the literal
> `$AP_SRC` / "migration plan" / PASS-REJECT wording. They work as-is via
> the mapping table above; a later pass can rewrite them in the
> coverage-check vocabulary directly.
