"""Lesson 14 — schedules (cron-style automation).

Two schedules demoed:
1. `nightly_full`   — fires every day at 02:00 (UTC by default; set
   `execution_timezone` to override)
2. `hourly_smoke`   — fires every hour, runs only a subset

Run with `dagster dev` so daemon is active:
    dagster dev -m cron_demo
Schedules listed at http://127.0.0.1:3000/schedules. Toggle "Start"
to enable. The daemon will fire materializations on schedule.

To force-fire for testing:
    dagster schedule list   -m cron_demo
    dagster schedule start  -m cron_demo nightly_full
    # or via UI: click "Run now"
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    DataVersion,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)


OUT_DIR = Path("/tmp/dagster-14-schedules")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _payload(label: str) -> MaterializeResult:
    data = label.encode()
    out = OUT_DIR / f"{label.replace(':', '_')}.out"
    out.write_text(label)
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(data).hexdigest()[:16]),
        metadata={"label": label, "path": str(out)},
    )


@asset
def metrics_extract(context: AssetExecutionContext) -> MaterializeResult:
    """Cheap asset; runs hourly in the smoke schedule."""
    return _payload("metrics_extract")


@asset(deps=[AssetKey("metrics_extract")])
def metrics_transform(context: AssetExecutionContext) -> MaterializeResult:
    return _payload("metrics_transform")


@asset
def nightly_report(context: AssetExecutionContext) -> MaterializeResult:
    """Heavy asset; runs once per day."""
    return _payload("nightly_report")


# ── Jobs (asset selections) ────────────────────────────────────

hourly_job = define_asset_job(
    name="hourly_job",
    selection=AssetSelection.assets(metrics_extract, metrics_transform),
)

nightly_job = define_asset_job(
    name="nightly_job",
    selection=AssetSelection.all(),     # all assets in this code location
)


# ── Schedules ──────────────────────────────────────────────────

hourly_smoke = ScheduleDefinition(
    name="hourly_smoke",
    job=hourly_job,
    cron_schedule="0 * * * *",          # at minute 0 of every hour
    execution_timezone="Asia/Taipei",
    description="Refresh cheap metrics every hour",
)

nightly_full = ScheduleDefinition(
    name="nightly_full",
    job=nightly_job,
    cron_schedule="0 2 * * *",          # 02:00 every day
    execution_timezone="Asia/Taipei",
    description="Full materialization including heavy nightly_report",
)


defs = Definitions(
    assets=[metrics_extract, metrics_transform, nightly_report],
    jobs=[hourly_job, nightly_job],
    schedules=[hourly_smoke, nightly_full],
)
