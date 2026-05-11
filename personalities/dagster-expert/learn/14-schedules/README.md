# lab14 · schedules — cron-style automation

**Time**: 30 min · **Prerequisites**: lessons 01, 02

## What's a schedule?

A `ScheduleDefinition` ties a **job** (asset selection) to a
**cron expression**. The Dagster daemon ticks every 30s and
fires runs when their cron expressions match.

Run with `dagster dev` so the daemon is alive. In production:
separate `dagster-daemon` process.

## Two schedules in this lesson

| Name | Cron | Asset selection | Use case |
|---|---|---|---|
| `hourly_smoke` | `0 * * * *` | `metrics_extract`, `metrics_transform` | refresh cheap deps every hour |
| `nightly_full` | `0 2 * * *` (02:00 Taipei) | all assets | full DAG rerun, includes `nightly_report` |

Cron format (5 fields):
```
minute  hour  day-of-month  month  day-of-week
```

Common patterns:
- `*/15 * * * *` — every 15 min
- `0 */6 * * *` — every 6 hours, on the hour
- `0 9 * * 1-5` — 09:00 weekdays
- `30 23 * * 0` — 23:30 on Sundays

## Run it

```bash
cd ~/projects/.../learn/14-schedules
dagster dev -m cron_demo
# open http://127.0.0.1:3000/schedules
```

Toggle "Start" on a schedule. Daemon will fire it on the next
cron tick. To force-fire for testing: click "Run now" in the UI.

CLI list / control:
```bash
dagster schedule list   -m cron_demo
dagster schedule status -m cron_demo nightly_full
dagster schedule start  -m cron_demo nightly_full
dagster schedule stop   -m cron_demo nightly_full
```

## Common gotchas

- **`execution_timezone` matters** — without it, cron is UTC.
  TSMC ops are GMT+8 (Asia/Taipei). The lesson sets it
  explicitly.
- **Daemon must be running** for schedules to fire. `dagster dev`
  has it; on prod use systemd `dagster-daemon run`.
- **A "Started" schedule won't backfire missed ticks** — if
  daemon was down at 02:00, the 02:00 run is just skipped. To
  catch up: trigger backfill manually.
- **No idempotency by default** — same cron tick may fire twice
  if the daemon clock is unstable. Use `RunRequest(run_key=...)`
  in a sensor instead for strict de-dupe.

## Schedule vs sensor

| Schedule | Sensor |
|---|---|
| Time-based | Event-based |
| Cron expression | Custom evaluation function |
| Fires every tick that matches | Fires when condition is met (cursor-tracked) |
| Use for: regular refreshes, EOD batches | Use for: external file appears, upstream finishes, etc. |

See lesson 15 for sensors.

## Related

- Lesson 15 (sensors) — event-driven triggers
- Lesson 16 (auto-materialize) — declarative reactive runs
- `dagster-expert/skills/dagster-yaml-reference/` — daemon config
