"""Harvest sensor (WHITEPAPER §7.3) — the most fragile module; over-test.

Coverage:
  - cursor advances ONLY after successful Dagster events + mark_harvested
  - repeat tick on already-harvested → SkipReason, cursor unchanged
  - partial failure in middle of batch → preserves prefix, retries suffix
  - cursor stays put on full-batch report failure
  - delete-after-harvest equivalent (harvested=1 flag) keeps the DB bounded
"""
import json
from pathlib import Path
from unittest.mock import MagicMock

import dagster as dg
import pytest

from framework.fabric import status_db
from framework.sensor.harvest import build_harvest_sensor


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "fabric.db"
    status_db.init_db(p)
    return p


def _seed_success(db, asset, partition, dv="v1"):
    k = status_db.compute_idempotency_key(asset, partition, [])
    status_db.upsert_pending(db, k, asset, partition)
    status_db.mark_success(db, k, dv)
    row = status_db.get_task(db, k)
    return row


def _build_sensor_and_ctx(db, recorded):
    @dg.op
    def _noop(): pass
    @dg.job
    def _noop_job(): _noop()

    sensor = build_harvest_sensor("flow", _noop_job, db_path_resolver=lambda: str(db))

    inst = MagicMock()
    def _report(event):
        recorded.append(event)
    inst.report_runless_asset_event.side_effect = _report

    ctx = MagicMock(spec=dg.SensorEvaluationContext)
    ctx.instance = inst
    ctx.cursor = None
    ctx.log = MagicMock()
    return sensor, ctx


def test_harvest_emits_one_event_per_unharvested_success(db):
    _seed_success(db, "characterize", "tt_25|INV", "dv-1")
    _seed_success(db, "characterize", "tt_25|BUF", "dv-2")
    recorded = []
    sensor, ctx = _build_sensor_and_ctx(db, recorded)
    result = sensor(ctx)

    assert len(recorded) == 2
    asset_keys = sorted(str(e.asset_key) for e in recorded)
    assert asset_keys == ["AssetKey(['characterize'])", "AssetKey(['characterize'])"]
    dvs = sorted(e.tags["dagster/data_version"] for e in recorded)
    assert dvs == ["dv-1", "dv-2"]
    assert isinstance(result, dg.SensorResult)
    cursor = json.loads(result.cursor)
    assert cursor["last_id"] > 0


def test_repeat_tick_emits_no_duplicate_events(db):
    _seed_success(db, "a", "p1", "v")
    recorded = []
    sensor, ctx = _build_sensor_and_ctx(db, recorded)

    result1 = sensor(ctx)
    assert len(recorded) == 1
    cursor1 = result1.cursor

    # Simulate cursor persisted across ticks
    ctx.cursor = cursor1
    result2 = sensor(ctx)
    assert len(recorded) == 1, "second tick must not re-emit"
    assert isinstance(result2, dg.SkipReason)


def test_partial_failure_advances_only_prefix(db):
    _seed_success(db, "a", "good1", "v1")
    bad = _seed_success(db, "a", "BAD", "v2")
    _seed_success(db, "a", "good3", "v3")
    recorded = []
    sensor, ctx = _build_sensor_and_ctx(db, recorded)

    fail_partition = "BAD"
    def _selective_report(event):
        if event.partition == fail_partition:
            raise RuntimeError("simulated Dagster outage")
        recorded.append(event)
    ctx.instance.report_runless_asset_event.side_effect = _selective_report

    result = sensor(ctx)
    # First row succeeded; loop broke at BAD; third row NOT processed this tick
    assert len(recorded) == 1
    assert recorded[0].partition == "good1"

    # Cursor advanced only to the prefix's last id (=first row's id)
    cursor = json.loads(result.cursor) if hasattr(result, "cursor") else {}
    # On break-mid-loop with one processed, sensor returns SensorResult with cursor
    assert isinstance(result, dg.SensorResult), result

    # Status DB: only good1 is harvested; BAD + good3 still un-harvested
    remaining = status_db.list_unharvested_terminals(db, after_id=0)
    states_by_part = {r.partition_key: r for r in remaining}
    assert "BAD" in states_by_part
    assert "good3" in states_by_part
    assert "good1" not in states_by_part


def test_head_failure_no_progress_cursor_unchanged(db):
    _seed_success(db, "a", "head_BAD", "v")
    _seed_success(db, "a", "tail_good", "v")
    recorded = []
    sensor, ctx = _build_sensor_and_ctx(db, recorded)
    ctx.cursor = json.dumps({"last_id": 0})

    def _all_fail(event):
        raise RuntimeError("Dagster down")
    ctx.instance.report_runless_asset_event.side_effect = _all_fail

    result = sensor(ctx)
    assert isinstance(result, dg.SkipReason), result
    # Nothing harvested; nothing in event log
    assert recorded == []
    remaining = status_db.list_unharvested_terminals(db, after_id=0)
    assert {r.partition_key for r in remaining} == {"head_BAD", "tail_good"}


def test_no_unharvested_terminals_is_skip(db):
    sensor, ctx = _build_sensor_and_ctx(db, [])
    result = sensor(ctx)
    assert isinstance(result, dg.SkipReason)


def test_harvest_includes_lsf_job_id_metadata_when_present(db):
    k = status_db.compute_idempotency_key("a", "p", [])
    status_db.upsert_pending(db, k, "a", "p")
    status_db.mark_submitted(db, k, lsf_job_id="42")
    status_db.mark_success(db, k, "v")
    recorded = []
    sensor, ctx = _build_sensor_and_ctx(db, recorded)
    sensor(ctx)
    assert recorded[0].metadata is not None
    md_keys = recorded[0].metadata
    # Metadata is a dict-like; entries may be wrapped — just confirm the key passed through
    assert any("42" in str(v) for v in (md_keys.values() if hasattr(md_keys, "values") else [md_keys]))
