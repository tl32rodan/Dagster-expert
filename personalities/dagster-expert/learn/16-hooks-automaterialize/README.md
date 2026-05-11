# lab16 · hooks + auto-materialize policies

**Time**: 45 min · **Prerequisites**: lessons 02, 05 (failures), 14, 15

## Two automation primitives

### Hooks — callbacks on step events
`@success_hook` / `@failure_hook` fire when a step ends. Use
for:
- Slack / Pagerduty notifications
- Metric collection
- Post-mortem logging / debug capture
- Auto-ticket creation on failure

This lesson's hooks write to `/tmp/dagster-16-out/notifications.log`
as a Slack-stand-in.

### Auto-materialize policies — declarative reactive runs
Attach `auto_materialize_policy=...` to an `@asset`. The daemon
evaluates the policy on each tick (~30s) and **fires a run if
the policy says yes**.

| Policy | Behavior |
|---|---|
| `AutoMaterializePolicy.eager()` | Materialize ASAP after upstream is fresh. Reactive. |
| `AutoMaterializePolicy.lazy()` | Materialize only when needed (downstream EAGER, or explicit ask). Lazy. |
| Custom rules | Skip during business hours, cap concurrent auto-mat runs, etc. |

EAGER: think of it as "always keep this asset current".
LAZY: "keep current only when consumers ask".

## The four assets in this lesson

| Asset | Policy | Why |
|---|---|---|
| `source_table` | EAGER | Root; whenever it's "stale" upstream, refresh |
| `derived_view` | EAGER | Reacts when `source_table` updates |
| `expensive_aggregate` | LAZY | Won't auto-rematerialize on derived_view updates; only when explicitly requested |
| `flaky` | (no policy) | Always fails; demonstrates failure_hook fires |

All four have `notify_success`/`notify_failure` hooks
registered.

## Run it

```bash
cd ~/projects/.../learn/16-hooks-automaterialize
dagster dev -m reactive
# open http://127.0.0.1:3000

# Click Materialize on `source_table`
# → success_hook fires → check /tmp/dagster-16-out/notifications.log
# → if EAGER policies are enabled: `derived_view` auto-fires next tick

# Click Materialize on `flaky`
# → STEP_FAILURE → failure_hook fires → log line records the err
```

Auto-materialize is OFF by default per asset. Toggle "Start" on
the **AutoMaterializePolicy** sidebar in the UI, OR materialize
manually to test hooks.

## Hooks recipe

```python
from dagster import HookContext, success_hook, failure_hook

@success_hook
def my_success_callback(context: HookContext) -> None:
    # context.op.name, context.run_id, context.op_config, ...
    send_slack(f"✓ {context.op.name} ok")

@failure_hook
def my_failure_callback(context: HookContext) -> None:
    err = context.op_exception   # the actual exception
    send_pagerduty(severity="warning",
                   summary=f"✗ {context.op.name}: {err}")
```

Attach to assets:

```python
@asset(hooks={my_success_callback, my_failure_callback})
def my_asset(): ...
```

Or globally to a job:

```python
@job(hooks={my_success_callback})
def my_job(): ...
```

## Auto-materialize policy decision tree

```
Does this asset need to be "always current"?
├── Yes → AutoMaterializePolicy.eager()
└── No
    └── Is it expensive AND derived from others?
        ├── Yes → AutoMaterializePolicy.lazy() (run only when asked)
        └── No  → omit policy (manual only)
```

EAGER common in: lookup tables, materialized views, downstream
char outputs that consumers expect fresh.

LAZY common in: expensive aggregations, end-of-day reports
(use a SCHEDULE for the "ask" trigger).

## TSMC use cases

- **Slack on failed char**: failure_hook + `slack-sdk` posts
  the run URL + error to a channel.
- **Auto-refresh derived libs**: `liberty_aggregate` with EAGER
  → whenever any PVT in `liberate_run` updates, aggregate
  refreshes automatically.
- **Expensive signoff report stays LAZY**: 4-hour summary job
  only runs when explicitly requested (manual click or schedule).

## Common gotchas

- **Hooks don't fire for unmaterialized assets** — if the asset
  never starts (e.g. upstream failed), no success/failure hook
  fires. Use a `run_status_sensor` instead for run-level events.
- **EAGER + many partitions = expensive** — every partition
  materialization triggers a daemon tick re-evaluation. For
  large partition counts (1000+), prefer LAZY + a schedule.
- **`HookContext.op_exception` is None for success_hook** —
  obviously, but worth stating.
- **Daemon must be running** for auto-materialize. `dagster dev`
  fine; on prod ensure `dagster-daemon run` is up.

## Related

- Lesson 14 (schedules) + 15 (sensors) — alternative
  trigger sources
- Lesson 05 (failures) — RetryPolicy interaction with
  failure_hook (hook fires AFTER all retries exhausted)
- `dagster-expert/skills/start-services/` — daemon ops
