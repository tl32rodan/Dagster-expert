"""Lesson 15 — sensors (event-driven automation).

Three sensor patterns demoed:
1. `@asset_sensor` — fire when an upstream asset materializes.
   Use case: lvf branch updates → trigger downstream char rerun.
2. `@sensor` (file-watch) — poll a watch directory; fire when
   new file appears. Use case: SOS-shelf check-in detected →
   trigger lib regen for affected cells.
3. `@run_status_sensor` — fire when ANOTHER job's run reaches
   a status. Use case: nightly_full finished → run downstream
   QA job.

Run with `dagster dev` (daemon needed for sensors):
    dagster dev -m event_demo

In UI: /sensors lists them, toggle "Start" to enable.
Daemon polls each sensor on its tick interval (default 30s).
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    DagsterRunStatus,
    DataVersion,
    Definitions,
    EventLogEntry,
    MaterializeResult,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset,
    asset_sensor,
    define_asset_job,
    run_status_sensor,
    sensor,
)


WATCH_DIR = Path("/tmp/dagster-15-watch")
WATCH_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR = Path("/tmp/dagster-15-out")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── Assets ─────────────────────────────────────────────────────

@asset
def lvf_source(context: AssetExecutionContext) -> MaterializeResult:
    """The 'upstream' asset_sensor watches."""
    out = OUT_DIR / "lvf_source.out"
    out.write_text("lvf data")
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"lvf").hexdigest()[:16]),
        metadata={"path": str(out)},
    )


@asset
def char_downstream(context: AssetExecutionContext) -> MaterializeResult:
    """Triggered by the asset_sensor when lvf_source updates."""
    out = OUT_DIR / "char_downstream.out"
    out.write_text("char from lvf")
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"char").hexdigest()[:16]),
        metadata={"path": str(out)},
    )


@asset
def shelf_handler(context: AssetExecutionContext) -> MaterializeResult:
    """Triggered when a new file appears in WATCH_DIR."""
    out = OUT_DIR / "shelf_handler.out"
    out.write_text("processed shelf check-in")
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"shelf").hexdigest()[:16]),
        metadata={"path": str(out)},
    )


@asset
def post_nightly_qa(context: AssetExecutionContext) -> MaterializeResult:
    """Triggered by run_status_sensor when nightly_job succeeds."""
    out = OUT_DIR / "post_nightly_qa.out"
    out.write_text("QA pass")
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(b"qa").hexdigest()[:16]),
        metadata={"path": str(out)},
    )


# ── Jobs ───────────────────────────────────────────────────────

char_job = define_asset_job("char_job", AssetSelection.assets(char_downstream))
shelf_job = define_asset_job("shelf_job", AssetSelection.assets(shelf_handler))
qa_job = define_asset_job("qa_job", AssetSelection.assets(post_nightly_qa))
nightly_job = define_asset_job("nightly_job", AssetSelection.assets(lvf_source))


# ── Sensors ────────────────────────────────────────────────────

@asset_sensor(asset_key=AssetKey("lvf_source"), job=char_job)
def lvf_updated_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry,
):
    """Fires whenever lvf_source materializes. Launches char_job.

    Returns a RunRequest with a unique run_key so the same
    materialization doesn't fire twice if the sensor restarts.
    """
    return RunRequest(
        run_key=f"lvf_{asset_event.dagster_event.event_specific_data.materialization.tags.get('dagster/data_version', 'x') if asset_event.dagster_event.event_specific_data.materialization.tags else 'x'}",
        tags={"source": "lvf_updated_sensor"},
    )


@sensor(job=shelf_job, minimum_interval_seconds=10)
def shelf_check_in_sensor(context: SensorEvaluationContext):
    """File-watch sensor: poll WATCH_DIR for new files.

    Fires shelf_job for each new file seen. Uses cursor to
    remember which files we've already processed across ticks.
    """
    cursor = context.cursor or "[]"
    import json
    seen = set(json.loads(cursor))

    files = sorted(p.name for p in WATCH_DIR.glob("*"))
    new_files = [f for f in files if f not in seen]

    if not new_files:
        return SkipReason(f"no new files (seen={len(seen)}, total={len(files)})")

    requests = [
        RunRequest(run_key=f"shelf_{f}", tags={"file": f})
        for f in new_files
    ]
    seen.update(new_files)
    return SensorResult(
        run_requests=requests,
        cursor=json.dumps(sorted(seen)),
    )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[nightly_job],
    request_job=qa_job,
)
def post_nightly_qa_sensor(context):
    """Fires when nightly_job (which materializes lvf_source) succeeds.

    Launches qa_job to validate the nightly outputs.
    """
    return RunRequest(
        run_key=f"qa_{context.dagster_run.run_id}",
        tags={"upstream_run": context.dagster_run.run_id},
    )


defs = Definitions(
    assets=[lvf_source, char_downstream, shelf_handler, post_nightly_qa],
    jobs=[char_job, shelf_job, qa_job, nightly_job],
    sensors=[lvf_updated_sensor, shelf_check_in_sensor, post_nightly_qa_sensor],
)
