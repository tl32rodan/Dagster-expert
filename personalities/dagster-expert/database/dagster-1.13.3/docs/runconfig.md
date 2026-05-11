# Run config — `Config` class + Launchpad

**Tested against Dagster 1.13.3.**

## When to use

Per-run knobs that aren't partition-shaped: debug toggles,
runtime overrides, "verbose this run", small policy choices.

If the value enumerates meaningful states ("for each corner"),
use partitions. If it's an orthogonal switch, use config.

## Define

```python
from dagster import Config, asset, MaterializeResult, AssetExecutionContext


class AuditConfig(Config):
    auditor: str = "anonymous"
    verbose: bool = False


@asset
def audited(
    context: AssetExecutionContext,
    config: AuditConfig,             # ← arg name "config" wires Pydantic schema
) -> MaterializeResult:
    if config.verbose:
        context.log.info(f"auditor={config.auditor}")
    ...
```

## Provide via UI Launchpad

UI → Materialize button → Launchpad opens YAML editor.

```yaml
ops:
  audited:               # asset name doubles as op name
    config:
      auditor: "alice"
      verbose: true
```

Click "Launch run". Dagster validates against the schema before
the run starts — typo'd field types fail fast, no wasted compute.

## Provide via CLI

```bash
dagster asset materialize -m mod --select audited \
    --config '{"ops": {"audited": {"config": {"auditor": "alice", "verbose": true}}}}'
```

Or:
```bash
dagster job execute -m mod -j __ASSET_JOB --config run_config.yaml
```

## `Config` field types — what's supported

```python
class MyConfig(Config):
    flag: bool = False
    name: str = "default"
    count: int = 1
    weight: float = 0.5
    items: list[str] = []
    mapping: dict[str, int] = {}
    nested: SubConfig                     # Config subclass
    optional: Optional[str] = None
```

Anything that maps to a Pydantic-supported type. Custom dataclass
extending `dagster.Config` works; arbitrary user classes don't.

## Common gotchas

- **`from __future__ import annotations` BREAKS Config schema
  introspection** — see `future-annotations-incompat.md`. Drop the
  future import in any file containing a `Config` subclass.
- **Defaults required for clickable launches** — without defaults,
  Launchpad refuses to launch with empty config. Set sensible
  defaults to allow bare clicks.
- **Config keys not appearing in Launchpad** — you defined `Config`
  but didn't add `config: MyConfig` arg to the asset function.

## Related

- [`asset-basics.md`](asset-basics.md) — `@asset` decorator
- [`future-annotations-incompat.md`](future-annotations-incompat.md)
- Examples: [`06_runconfig.py`](../examples/06_runconfig.py)
