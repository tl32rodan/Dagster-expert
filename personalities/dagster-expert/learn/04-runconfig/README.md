# lab4 · run config — parameterizing a run

**Time**: 30 min · **Prerequisites**: lessons 01, 02

## What you'll learn

- How to expose knobs to the user that they can set per-run
- Pydantic-style `Config` classes (Dagster 1.13's RunConfig API)
- The "Launchpad" in the UI
- When run config beats partitions, and vice versa

## Why run config exists

Sometimes a run needs **a parameter that's not partition-shaped**.
Examples:

- "Run with `verbose: true` for this debug session"
- "Use the staging Postgres instead of production"
- "Cap memory to 4GB for this slow run"
- "Run only the 5 highest-priority instances out of the 50"

You don't want a partition for every Boolean knob — the partition
strip becomes meaningless. You want a per-run config the user
fills in when they launch.

## The lesson in 60 seconds

```bash
cd 04-runconfig
dagster dev -m configured
# open http://127.0.0.1:3000
```

Click `audited_payload`. Click "Materialize". The UI offers the
**Launchpad** — a YAML editor where you can supply config:

```yaml
ops:
  audited_payload:
    config:
      auditor: "alice"
      verbose: true
```

Click "Launch run". The run logs show `auditor=alice verbose=True`.

Try again with different values.

## Run config vs partitions — when to use which

| Use partitions | Use run config |
|---|---|
| The set of values is enumerable + meaningful as fresh/stale axes | The value is per-run policy / debug toggle / runtime override |
| Each value should produce a separate materialization record | The value affects HOW the materialization happens, not WHAT it represents |
| You want a backfill button | You don't expect to "backfill `verbose=true`" |

You can use BOTH on the same asset: partitioned + configurable.

## Now try

### Try 1 · Set verbose=false

Edit the launchpad YAML to `verbose: false`. The asset's log
output gets quieter.

### Try 2 · Trigger a config-validation error

Set `auditor: 123` (a number). Try to launch. The UI rejects with
"Field `auditor` must be a string" — Dagster validates BEFORE the
run starts. You don't waste compute on a typo.

### Try 3 · Save a config preset

Click the launchpad's "Save preset". Name it `verbose-debug`. Now
when launching, you can pick this preset instead of typing YAML
each time.

## Common pitfalls

- **"Config schema is required"**: Dagster requires a config when
  the asset declares one. The launchpad refuses to launch with
  empty config. Fill in defaults in the `Config` class to allow
  bare clicks.
- **Config keys not appearing in UI**: you defined the `Config`
  class but didn't add `config: MyConfig` as a function arg. The
  arg is what wires it in.
- **Default values silently ignored**: you set `verbose: bool =
  True` but the UI shows it as required. You forgot `Optional` or
  `= Field(default=...)`. In 1.13.3, plain `= True` works in the
  Pydantic class — see code below.

## Cheat sheet

```python
from dagster import asset, Config, MaterializeResult, AssetExecutionContext

class AuditConfig(Config):
    auditor: str = "anonymous"
    verbose: bool = False

@asset
def audited_payload(
    context: AssetExecutionContext,
    config: AuditConfig,
) -> MaterializeResult:
    if config.verbose:
        context.log.info(f"auditor={config.auditor}")
    ...
```

The Launchpad YAML the user fills in:

```yaml
ops:
  audited_payload:                 # asset name doubles as op name
    config:
      auditor: "alice"
      verbose: true
```

CLI equivalent (rare but handy for scripted runs):

```bash
dagster asset materialize -w workspace.yaml --select audited_payload \
    --config '{"ops": {"audited_payload": {"config": {"auditor": "alice"}}}}'
```
