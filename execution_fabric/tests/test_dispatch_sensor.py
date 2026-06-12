"""Dispatch sensor (WHITEPAPER §3.3): desired − observed → RunRequest.

`observed` MUST come from status DB (not Dagster materializations) — see
R7 placeholder hazard in the whitepaper.
"""
from unittest.mock import MagicMock

import dagster as dg
import pytest

from framework.fabric import status_db
from framework.sensor.dispatch import build_dispatch_sensor
from framework.spec.schema import FlowSpec


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "fabric.db"
    status_db.init_db(p)
    return p


def _spec():
    return FlowSpec.model_validate({
        "version": 1, "flow_name": "f",
        "dimensions": {
            "pvt": {"type": "static", "values": ["A", "B"]},
            "cell": {"type": "static", "values": ["X", "Y"]},
        },
        "lsf": {"default": {"queue": "normal", "cores": 4, "mem_mb": 4096, "walltime": "1:00"}},
        "assets": [
            {"name": "start", "kind": "entry"},
            {"name": "c", "kind": "compute",
             "script": "framework.versioning.base:content_hash_version",
             "partitioned_by": ["pvt", "cell"],
             "depends_on": [{"asset": "start", "mapping": "all"}]},
        ],
    })


@dg.op
def _noop_op(): pass

@dg.job
def _noop_job(): _noop_op()


def test_dispatch_emits_runrequests_for_all_missing(db):
    spec = _spec()
    sensor = build_dispatch_sensor(
        spec, [spec.assets[1]], job=_noop_job,
        db_path_resolver=lambda: str(db),
    )
    ctx = MagicMock(spec=dg.SensorEvaluationContext)
    ctx.cursor = None
    result = sensor(ctx)
    # 2 pvt × 2 cell = 4 RunRequests. MultiPartitionsDefinition orders
    # dimensions alphabetically (cell, then pvt), so keys are "cell|pvt".
    assert isinstance(result, list)
    assert len(result) == 4
    keys = sorted(rr.partition_key for rr in result)
    assert keys == ["X|A", "X|B", "Y|A", "Y|B"]
    # run_key dedups across ticks
    assert all(rr.run_key for rr in result)


def test_dispatch_skips_observed_partitions(db):
    spec = _spec()
    # Pre-record SUCCESS for X|A in status DB (cell|pvt alphabetical order)
    k = status_db.compute_idempotency_key("c", "X|A", [])
    status_db.upsert_pending(db, k, "c", "X|A")
    status_db.mark_success(db, k, "v")

    sensor = build_dispatch_sensor(
        spec, [spec.assets[1]], job=_noop_job,
        db_path_resolver=lambda: str(db),
    )
    ctx = MagicMock(spec=dg.SensorEvaluationContext)
    ctx.cursor = None
    result = sensor(ctx)
    assert isinstance(result, list)
    keys = sorted(rr.partition_key for rr in result)
    assert "X|A" not in keys
    assert keys == ["X|B", "Y|A", "Y|B"]


def test_dispatch_skip_when_all_observed(db):
    spec = _spec()
    for p in ["X|A", "X|B", "Y|A", "Y|B"]:
        k = status_db.compute_idempotency_key("c", p, [])
        status_db.upsert_pending(db, k, "c", p)
        status_db.mark_success(db, k, "v")
    sensor = build_dispatch_sensor(
        spec, [spec.assets[1]], job=_noop_job,
        db_path_resolver=lambda: str(db),
    )
    ctx = MagicMock(spec=dg.SensorEvaluationContext)
    ctx.cursor = None
    result = sensor(ctx)
    assert isinstance(result, dg.SkipReason)


def test_dispatch_run_keys_are_stable_across_ticks(db):
    spec = _spec()
    sensor = build_dispatch_sensor(
        spec, [spec.assets[1]], job=_noop_job,
        db_path_resolver=lambda: str(db),
    )
    ctx = MagicMock(spec=dg.SensorEvaluationContext)
    ctx.cursor = None
    r1 = sensor(ctx)
    r2 = sensor(ctx)
    rk1 = sorted(rr.run_key for rr in r1)
    rk2 = sorted(rr.run_key for rr in r2)
    assert rk1 == rk2, "Dagster dedups by run_key — must be stable across ticks"
