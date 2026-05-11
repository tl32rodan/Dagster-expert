"""Lesson 16 — hooks + auto-materialize policies.

Two automation primitives that complement schedules/sensors:

## Hooks
`@success_hook` / `@failure_hook` — callbacks that fire when a
step succeeds or fails. Use for notifications (Slack/Pagerduty
mock here just writes a marker file), metric collection,
post-mortem logging.

Hooks are op-level (asset-level too — same decorator).

## Auto-materialize policies
Declarative "this asset should be materialized when X is true".
The daemon evaluates the policy on each tick and triggers runs.

- `AutoMaterializePolicy.eager()` — materialize as soon as
  upstream is fresh (reactive).
- `AutoMaterializePolicy.lazy()` — only when needed (e.g. a
  schedule asks for downstream).
- Custom rules: don't auto-materialize during business hours,
  cap concurrent auto-mat runs, etc.

Run with `dagster dev -m reactive` (daemon needed for both
auto-materialize and hooks-on-async-runs).
"""

import hashlib
import time
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutoMaterializePolicy,
    DataVersion,
    Definitions,
    HookContext,
    MaterializeResult,
    asset,
    failure_hook,
    success_hook,
)


OUT_DIR = Path("/tmp/dagster-16-out")
OUT_DIR.mkdir(parents=True, exist_ok=True)
NOTIFY_LOG = OUT_DIR / "notifications.log"


# ── Hook callbacks (Slack/Pagerduty mock) ──────────────────────

@success_hook
def notify_success(context: HookContext) -> None:
    line = (
        f"[{time.strftime('%H:%M:%S')}] "
        f"✓ {context.op.name} succeeded "
        f"(run={context.run_id[:8]})\n"
    )
    with NOTIFY_LOG.open("a") as f:
        f.write(line)
    context.log.info(f"notify_success: {line.strip()}")


@failure_hook
def notify_failure(context: HookContext) -> None:
    line = (
        f"[{time.strftime('%H:%M:%S')}] "
        f"✗ {context.op.name} FAILED "
        f"(run={context.run_id[:8]}) — "
        f"err: {context.op_exception}\n"
    )
    with NOTIFY_LOG.open("a") as f:
        f.write(line)
    context.log.error(f"notify_failure: {line.strip()}")


# ── Assets with auto-materialize + hooks ───────────────────────

@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    hooks={notify_success, notify_failure},
)
def source_table() -> MaterializeResult:
    """Root. EAGER auto-materialize: daemon will run this on
    schedule (e.g. when something else triggers it being needed).
    """
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"src").hexdigest()[:16]),
        metadata={"role": "root"},
    )


@asset(
    deps=[AssetKey("source_table")],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    hooks={notify_success, notify_failure},
)
def derived_view(context: AssetExecutionContext) -> MaterializeResult:
    """EAGER: when source_table updates, this auto-rematerializes."""
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"derived").hexdigest()[:16]),
        metadata={"role": "derived"},
    )


@asset(
    deps=[AssetKey("derived_view")],
    auto_materialize_policy=AutoMaterializePolicy.lazy(),
    hooks={notify_success, notify_failure},
)
def expensive_aggregate(context: AssetExecutionContext) -> MaterializeResult:
    """LAZY: only materialized when someone explicitly asks
    (i.e. a schedule asks for it, or a downstream EAGER asset
    needs it). Won't auto-rerun on derived_view updates.
    """
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"agg").hexdigest()[:16]),
        metadata={"role": "expensive_aggregate"},
    )


@asset(hooks={notify_success, notify_failure})
def flaky() -> MaterializeResult:
    """Demonstrate failure hook: this asset always fails."""
    raise RuntimeError("intentional failure for hook demo")


defs = Definitions(
    assets=[source_table, derived_view, expensive_aggregate, flaky],
)
