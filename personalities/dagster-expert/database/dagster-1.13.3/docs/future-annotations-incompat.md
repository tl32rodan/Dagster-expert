# `from __future__ import annotations` — Dagster 1.13.3 incompatibility

**Tested against Dagster 1.13.3.**

## Symptom

```
dagster._core.errors.DagsterInvalidDefinitionError: Cannot
annotate `context` parameter with type AssetExecutionContext.
`context` must be annotated with AssetExecutionContext,
AssetCheckExecutionContext, OpExecutionContext, or left blank.
```

OR for `Config` classes:

```
dagster._core.errors.DagsterInvalidPythonicConfigDefinitionError:
Unable to resolve config type 'AuditConfig' to a supported
Dagster config type.
```

The error message contradicts itself ("must be annotated with X"
when you DID annotate with X) — a giveaway that Dagster's
reflection isn't seeing the actual class.

## Root cause

```python
from __future__ import annotations              # ← THE PROBLEM
from dagster import AssetExecutionContext, asset

@asset
def my_asset(context: AssetExecutionContext): ...
```

PEP 563 / `from __future__ import annotations` makes ALL
annotations evaluated as **strings** (deferred evaluation).
Dagster 1.13.3's reflection does identity comparison against
the actual `AssetExecutionContext` class — string `"AssetExecutionContext"`
fails the check.

Same root cause for `Config` classes: Pydantic-style schema
introspection in `dagster.Config` reads field annotations; under
PEP 563 they're strings, not types.

## Fix

Drop `from __future__ import annotations` from any module that
defines `@asset` / `@op` with annotated `context` parameters or
defines `Config` subclasses.

```python
# Don't import the future
from dagster import AssetExecutionContext, asset

@asset
def my_asset(context: AssetExecutionContext): ...   # works
```

Alternative: leave `context` un-annotated:

```python
from __future__ import annotations               # OK if context is bare
from dagster import asset

@asset
def my_asset(context): ...                        # works (no annotation to mis-evaluate)
```

But this gives up type-checker support for context. Dropping the
future import is cleaner.

## Why it matters for Brian's setup

`from __future__ import annotations` is the modern Python style
(default in PEP 649 / 3.13+). Most templates / cookiecutter / IDE
auto-imports include it. Dagster 1.13.3 was released before
runtime-eval semantics caught up with PEP 563 universally; later
Dagster versions may handle it cleanly.

Until upgrade, **ban `from __future__ import annotations` in any
file containing `@asset`, `@op`, or `Config` subclass**. Add a
linter rule (ruff) or pre-commit check to catch it.

## Detection one-liner

```bash
# In a Dagster project, find files that have BOTH the future import
# AND define a Config or @asset:
grep -rEl "from __future__ import annotations" \
  $(grep -rEl "@asset|class .*\(Config\)" --include='*.py') \
  2>/dev/null
```

Output is files at risk.

## Related

- Dagster issue tracker (search "PEP 563" or "from __future__
  annotations")
- Examples: [`01_basic_asset.py`](../examples/01_basic_asset.py)
  — note the absence of the future import
