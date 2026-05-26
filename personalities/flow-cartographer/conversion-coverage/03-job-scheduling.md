<!-- all-might generated -->
# Coverage: 03 — Job Scheduling

Scope: the source flow's job scheduling, recurrence, event-driven
triggering, and backfill model versus Dagster 1.13.3's
`ScheduleDefinition`, sensors (`@sensor`, `@asset_sensor`,
`@run_status_sensor`), partitions, backfills, and the dagster-daemon
process.

## Flow behavior (must cite from $FLOW_SRC)

Required reading paths (use `grep -rn "<keyword>" $FLOW_SRC`):
- Scheduler module (search `schedule`, `cron`, `trigger`, `recurring`)
- Sensor / event-driven trigger module (search `sensor`, `event`,
  `watch`, `poll`)
- Backfill / batch-rerun module (search `backfill`, `batch`,
  `historical`)
- Daemon / scheduler process module (search `daemon`, `tick`,
  `interval`)

Expected behaviors (flow-side, to be confirmed by `$FLOW_SRC` reading):
- B1: Time-based recurrence is expressible (cron-like or equivalent
  spec).
- B2: Event-based triggering exists (file landed, upstream completed,
  external signal).
- B3: Recurrence runs in a long-lived daemon process distinct from the
  webserver / API.
- B4: Backfill semantics (re-run a recurrence over historical periods)
  are supported, with deduplication keys per period.
- B5: Schedules / sensors can be started / stopped / their state
  inspected externally (UI / CLI / API).

## Dagster 1.13.3 corresponding API

Source:
- `personalities/dagster-expert/database/dagster-1.13.3/docs/partitions.md`
  (for partitioned schedules / backfills)
- `personalities/dagster-expert/database/dagster-1.13.3/docs/INDEX.md`
  (use the schedule / sensor topic entries)

Also see:
- `personalities/dagster-expert/learn/14-schedules/README.md`
- `personalities/dagster-expert/learn/15-sensors/README.md`
- `personalities/dagster-expert/learn/16-hooks-automaterialize/README.md`
- `personalities/dagster-expert/skills/cli-cheatsheet/SKILL.md`
  (`dagster schedule …`, `dagster sensor …` subcommands)

Public APIs / classes / CLI commands:
- `ScheduleDefinition(job=..., cron_schedule="5 0 * * *",
  execution_timezone="Asia/Taipei", minimum_interval_seconds=...)`
  — cite `learn/14-schedules/README.md`
- `@sensor(minimum_interval_seconds=...)`,
  `@asset_sensor(asset_key=AssetKey(...))`,
  `@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)` — cite
  `learn/15-sensors/README.md`
- `SensorResult(run_requests=[RunRequest(run_key=...,
  partition_key=...)], cursor=...)` — cite
  `learn/15-sensors/README.md`
- Backfill via partitions UI / CLI:
  `dagster asset backfill --partition-range=... --select=...` — cite
  `skills/cli-cheatsheet/SKILL.md` and `docs/partitions.md`
- Daemon process: `dagster-daemon run` — cite
  `skills/start-services/SKILL.md`
- Schedule / sensor CLI: `dagster schedule list`, `dagster schedule
  start <name>`, `dagster schedule stop <name>`, `dagster sensor
  list`, `dagster sensor cursor` — cite
  `skills/cli-cheatsheet/SKILL.md`

## Coverage criteria (covered only if ALL true)

- [ ] C1: The flow's cron / recurrence spec is mapped onto
  `ScheduleDefinition(cron_schedule=..., execution_timezone=...)` row
  by row.
- [ ] C2: The flow's event-trigger semantics are mapped onto exactly
  one of `@sensor`, `@asset_sensor`, `@run_status_sensor`, with
  justification per flow trigger.
- [ ] C3: The flow's daemon / scheduler process is mapped onto
  `dagster-daemon run`, with the increment naming the `dagster.yaml`
  configuration that turns it on.
- [ ] C4: The flow's backfill semantics are mapped onto Dagster
  partition backfill (`dagster asset backfill ...`), with deduplication
  semantics shown (Dagster's `run_key` on `RunRequest` vs the flow's
  dedup key).
- [ ] C5: The flow's schedule / sensor start/stop/inspect verbs are
  mapped onto `dagster schedule {list,start,stop}` and `dagster sensor
  {list,cursor}` CLI commands.
- [ ] C6: Timezone semantics match — the flow's recurrence timezone is
  declared and mapped onto Dagster's `execution_timezone` parameter.
- [ ] C7: Minimum-interval semantics match — the flow's polling /
  rate-limiting parameter is mapped onto
  `minimum_interval_seconds=...`.

## Gap triggers (mechanical)

Each criterion is **covered** (the increment cites the mapping) or a
**gap**. An unaddressed gap is a `coverage-gap` finding (verify check 6
FAILs); a gap explicitly parked in `flow-model/_open_questions.yaml` is
acceptable, not a hard reject. Each remediation below is how to *cover*
the criterion — parking it as an open question is the documented
alternative.

- C1 gap → `coverage-gap 03.C1: flow cron spec not mapped row-by-row to
  ScheduleDefinition. Remediation: list each flow schedule with its
  cron string, and the corresponding ScheduleDefinition call.`
- C2 gap → `coverage-gap 03.C2: flow event triggers not categorized into
  @sensor / @asset_sensor / @run_status_sensor. Remediation: pick one
  Dagster sensor type per flow trigger and cite learn/15-sensors/.`
- C3 gap → `coverage-gap 03.C3: flow daemon not mapped onto
  dagster-daemon process. Remediation: cite skills/start-services/SKILL.md
  and the dagster.yaml stanza that enables the daemon.`
- C4 gap → `coverage-gap 03.C4: flow backfill not mapped onto partition
  backfill. Remediation: show the partition shape, run_key strategy,
  and the dagster asset backfill CLI invocation.`
- C5 gap → `coverage-gap 03.C5: flow control verbs not mapped onto
  schedule/sensor CLI. Remediation: list each flow verb (list/start/
  stop/cursor) with its dagster CLI counterpart.`
- C6 gap → `coverage-gap 03.C6: timezone semantics not specified.
  Remediation: declare flow timezone, cite execution_timezone= in the
  ScheduleDefinition row.`
- C7 gap → `coverage-gap 03.C7: minimum interval not mapped.
  Remediation: cite flow polling spec and minimum_interval_seconds=
  argument.`

## Evidence template

| Criterion | Flow source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $FLOW_SRC/... | learn/14-schedules/README.md::... | covered / gap |
| C2 | $FLOW_SRC/... | learn/15-sensors/README.md::... | covered / gap |
| C3 | $FLOW_SRC/... | skills/start-services/SKILL.md::dagster-daemon | covered / gap |
| C4 | $FLOW_SRC/... | skills/cli-cheatsheet/SKILL.md::asset backfill | covered / gap |
| C5 | $FLOW_SRC/... | skills/cli-cheatsheet/SKILL.md::schedule/sensor | covered / gap |
| C6 | $FLOW_SRC/... | learn/14-schedules/README.md::execution_timezone | covered / gap |
| C7 | $FLOW_SRC/... | learn/15-sensors/README.md::minimum_interval_seconds | covered / gap |
