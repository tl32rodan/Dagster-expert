<!-- all-might generated -->
# Audit: 03 — Job Scheduling

Scope: AP's job scheduling, recurrence, event-driven triggering, and
backfill model versus Dagster 1.13.3's `ScheduleDefinition`, sensors
(`@sensor`, `@asset_sensor`, `@run_status_sensor`), partitions,
backfills, and the dagster-daemon process.

## AP behavior (must cite from $AP_SRC)

Required reading paths (use `grep -rn "<keyword>" $AP_SRC`):
- Scheduler module (search `schedule`, `cron`, `trigger`, `recurring`)
- Sensor / event-driven trigger module (search `sensor`, `event`,
  `watch`, `poll`)
- Backfill / batch-rerun module (search `backfill`, `batch`,
  `historical`)
- Daemon / scheduler process module (search `daemon`, `tick`,
  `interval`)

Expected behaviors (AP-side, to be confirmed by `$AP_SRC` reading):
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

## Parity criteria (PASS only if ALL true)

- [ ] C1: AP cron / recurrence spec is mapped onto
  `ScheduleDefinition(cron_schedule=..., execution_timezone=...)` row
  by row.
- [ ] C2: AP event-trigger semantics are mapped onto exactly one of
  `@sensor`, `@asset_sensor`, `@run_status_sensor`, with justification
  per AP trigger.
- [ ] C3: AP daemon / scheduler process is mapped onto
  `dagster-daemon run`, with the plan naming the `dagster.yaml`
  configuration that turns it on.
- [ ] C4: AP backfill semantics are mapped onto Dagster partition
  backfill (`dagster asset backfill ...`), with deduplication
  semantics shown (Dagster's `run_key` on `RunRequest` vs AP's dedup
  key).
- [ ] C5: AP schedule / sensor start/stop/inspect verbs are mapped
  onto `dagster schedule {list,start,stop}` and `dagster sensor
  {list,cursor}` CLI commands.
- [ ] C6: Timezone semantics match — AP's recurrence timezone is
  declared and mapped onto Dagster's `execution_timezone` parameter.
- [ ] C7: Minimum-interval semantics match — AP's polling /
  rate-limiting parameter is mapped onto
  `minimum_interval_seconds=...`.

## Refusal triggers (mechanical)

- C1 unmet → `REJECT: 03.C1: AP cron spec not mapped row-by-row to
  ScheduleDefinition. Remediation: list each AP schedule with its
  cron string, and the corresponding ScheduleDefinition call.`
- C2 unmet → `REJECT: 03.C2: AP event triggers not categorized into
  @sensor / @asset_sensor / @run_status_sensor. Remediation: pick one
  Dagster sensor type per AP trigger and cite learn/15-sensors/.`
- C3 unmet → `REJECT: 03.C3: AP daemon not mapped onto dagster-daemon
  process. Remediation: cite skills/start-services/SKILL.md and the
  dagster.yaml stanza that enables the daemon.`
- C4 unmet → `REJECT: 03.C4: AP backfill not mapped onto partition
  backfill. Remediation: show the partition shape, run_key strategy,
  and the dagster asset backfill CLI invocation.`
- C5 unmet → `REJECT: 03.C5: AP control verbs not mapped onto
  schedule/sensor CLI. Remediation: list each AP verb (list/start/
  stop/cursor) with its dagster CLI counterpart.`
- C6 unmet → `REJECT: 03.C6: timezone semantics not specified.
  Remediation: declare AP timezone, cite execution_timezone= in the
  ScheduleDefinition row.`
- C7 unmet → `REJECT: 03.C7: minimum interval not mapped.
  Remediation: cite AP polling spec and minimum_interval_seconds=
  argument.`

## Evidence template

| Criterion | AP source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $AP_SRC/... | learn/14-schedules/README.md::... | PASS / FAIL |
| C2 | $AP_SRC/... | learn/15-sensors/README.md::... | PASS / FAIL |
| C3 | $AP_SRC/... | skills/start-services/SKILL.md::dagster-daemon | PASS / FAIL |
| C4 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::asset backfill | PASS / FAIL |
| C5 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::schedule/sensor | PASS / FAIL |
| C6 | $AP_SRC/... | learn/14-schedules/README.md::execution_timezone | PASS / FAIL |
| C7 | $AP_SRC/... | learn/15-sensors/README.md::minimum_interval_seconds | PASS / FAIL |
