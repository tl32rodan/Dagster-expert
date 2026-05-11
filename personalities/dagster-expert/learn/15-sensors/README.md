# lab15 · sensors — event-driven automation

**Time**: 45 min · **Prerequisites**: lessons 02 (deps), 14 (schedules)

## What's a sensor?

A `@sensor` is a function the Dagster daemon calls every ~30s.
It returns a `RunRequest` (fire a job) or `SkipReason` (nothing
to do this tick). Cursors let sensors remember state across
ticks.

Three flavors covered in this lesson:

| Flavor | Decorator | Fires when |
|---|---|---|
| Asset sensor | `@asset_sensor(asset_key=...)` | Specific asset materializes |
| Generic sensor | `@sensor` | Custom condition you write (file watch, API check, etc.) |
| Run-status sensor | `@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)` | Another job's run reaches a state |

## Three demos in this lesson

### 1. `lvf_updated_sensor` (`@asset_sensor`)
Watches `lvf_source` materialization. When it fires, launches
`char_job` (materializes `char_downstream`). Run-key dedupes by
data_version so the same materialization can't fire twice.

### 2. `shelf_check_in_sensor` (`@sensor`)
Polls `WATCH_DIR=/tmp/dagster-15-watch/` every 10s. New files
trigger `shelf_job` (one run per new file). Cursor tracks seen
filenames as JSON.

Test:
```bash
touch /tmp/dagster-15-watch/cell_A_v2.lib
# wait ~10s — sensor ticks, sees new file, fires shelf_job
touch /tmp/dagster-15-watch/cell_B_v1.lib
# next tick, fires shelf_job for B (not A — already seen)
```

### 3. `post_nightly_qa_sensor` (`@run_status_sensor`)
Fires when `nightly_job` succeeds. Launches `qa_job`. Useful
for "validation step after a heavy batch finishes".

## Run it

```bash
cd ~/projects/.../learn/15-sensors
dagster dev -m event_demo
# open http://127.0.0.1:3000/sensors
```

Toggle each sensor "Start". Daemon will poll every minimum
interval (10s for shelf, 30s default for others).

CLI:
```bash
dagster sensor list   -m event_demo
dagster sensor cursor -m event_demo shelf_check_in_sensor
dagster sensor start  -m event_demo lvf_updated_sensor
```

## Cursor — the stateful sensor pattern

Sensors are stateless functions BY DEFAULT. To track "what have
I already processed", store JSON in `context.cursor` and return
`SensorResult(run_requests=[...], cursor=<new>)`.

The cursor persists in Dagster's instance DB. Restarts don't
lose it. Reset via UI or `dagster sensor reset-cursor`.

## Common gotchas

- **`run_key` is your dedupe key**, not the cursor. If a sensor
  fires twice for the same event (e.g. daemon restart), same
  `run_key` → second request is skipped. Use run_key religiously.
- **Sensor tick frequency** — `minimum_interval_seconds` is the
  FLOOR; daemon may tick slower under load. Set to ≥10 for
  expensive evaluators (DB queries, HTTP).
- **Long-running sensor body** — sensor evaluations must finish
  in seconds. If your sensor calls an external API, cache + bail
  fast on errors.
- **`@asset_sensor` only fires on TOP-LEVEL asset materializations**
  by default. For partition-level fires, use the multi-asset
  variant (advanced).

## TSMC use cases

- **Shelf check-in detector**: SOS shelf write → sensor fires →
  affected libs regenerated. (Pattern: poll `find /shelf -newer
  $cursor_time -name "*.cell"`.)
- **Upstream finishes → start downstream**: foundry team's
  signoff PR job ends → kick off downstream lib build.
- **Daily license usage report**: sensor checks LSF accounting
  every hour, RunRequest the report job if usage > threshold.

## Related

- Lesson 14 (schedules) — time-based alternative
- Lesson 16 (auto-materialize) — declarative reactive (often
  replaces simple sensors)
