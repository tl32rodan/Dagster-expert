"""LSF run client (WHITEPAPER §3.4 lsf_run_client).

Tests argv assembly + dispatch flow with the mock bsub from
`tests/_mock_lsf/bsub.py`. End-to-end LSF dispatch is covered by
`scripts/run_demo.py` (live daemon).
"""
import os
import re
import sys
from pathlib import Path

import pytest

from framework.fabric import lsf_run_client, status_db

MOCK_BSUB = Path(__file__).resolve().parent / "_mock_lsf" / "bsub.py"


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "fabric.db"
    status_db.init_db(p)
    return p


def test_build_bsub_argv_includes_required_flags(tmp_path):
    cfg = lsf_run_client.LSFConfig(
        queue="premium", cores=16, mem_mb=32768, walltime="12:00", project="P1"
    )
    argv = lsf_run_client._build_bsub_argv(
        idempotency_key="abcdef123456",
        inner_argv=["echo", "hi"],
        fabric_worker_path="/abs/fabric_worker.py",
        db_path=str(tmp_path / "x.db"),
        asset_name="characterize",
        partition_key="tt_25|INV",
        lsf_cfg=cfg,
        log_dir=str(tmp_path / "logs"),
        bsub_bin="bsub",
    )
    # Standard bsub flags
    assert "bsub" in argv
    assert "-q" in argv and argv[argv.index("-q") + 1] == "premium"
    assert "-n" in argv and argv[argv.index("-n") + 1] == "16"
    assert any("rusage[mem=32768]" in a for a in argv)
    assert "-W" in argv and argv[argv.index("-W") + 1] == "12:00"
    assert "-P" in argv and argv[argv.index("-P") + 1] == "P1"
    # NO -K — non-blocking is the whole point
    assert "-K" not in argv
    # Idempotency key flows through
    assert "--idempotency-key" in argv
    assert argv[argv.index("--idempotency-key") + 1] == "abcdef123456"
    # Inner argv is at the tail after `--`
    sep = argv.index("--")
    assert argv[sep + 1:] == ["echo", "hi"]


def test_build_bsub_argv_invoker_prefix(tmp_path):
    """Invoker (e.g. [sys.executable]) goes before bsub_bin so the mock
    .py runs without shebang + executable bit + PATH."""
    cfg = lsf_run_client.LSFConfig()
    argv = lsf_run_client._build_bsub_argv(
        idempotency_key="k",
        inner_argv=["x"],
        fabric_worker_path="/abs/w.py",
        db_path=str(tmp_path / "x.db"),
        asset_name="a",
        partition_key="p",
        lsf_cfg=cfg,
        log_dir=str(tmp_path / "logs"),
        bsub_bin="/abs/mock_bsub.py",
        invoker=["/usr/bin/python3"],
    )
    assert argv[0] == "/usr/bin/python3"
    assert argv[1] == "/abs/mock_bsub.py"


def test_parse_job_id_matches_standard_lsf_output():
    assert lsf_run_client._parse_job_id("Job <12345> is submitted to queue <normal>.") == "12345"
    assert lsf_run_client._parse_job_id("Job 99999 is submitted to queue normal.") == "99999"
    assert lsf_run_client._parse_job_id("garbage") is None


def test_dispatch_writes_pending_then_submitted_with_job_id(db, tmp_path):
    # Point the mock at a writable record dir so we don't bleed into the
    # default `_mock_lsf` test mode.
    os.environ.pop("MOCK_LSF_RECORD", None)
    cfg = lsf_run_client.LSFConfig()
    idem = status_db.compute_idempotency_key("a", "p", [])
    job_id = lsf_run_client.dispatch(
        idempotency_key=idem,
        asset_name="a", partition_key="p",
        inner_argv=["true"],
        fabric_worker_path="/abs/fabric_worker.py",
        db_path=str(db),
        lsf_cfg=cfg,
        log_dir=str(tmp_path / "logs"),
        bsub_bin=str(MOCK_BSUB),
        invoker=[sys.executable],
    )
    assert job_id and re.match(r"\d+", job_id)
    row = status_db.get_task(db, idem)
    assert row.state == status_db.STATE_SUBMITTED
    assert row.lsf_job_id == job_id


def test_dispatch_dedups_via_unique_constraint(db, tmp_path):
    cfg = lsf_run_client.LSFConfig()
    idem = status_db.compute_idempotency_key("a", "p", [])
    lsf_run_client.dispatch(
        idempotency_key=idem,
        asset_name="a", partition_key="p",
        inner_argv=["true"],
        fabric_worker_path="/abs/fw.py",
        db_path=str(db),
        lsf_cfg=cfg,
        log_dir=str(tmp_path / "logs1"),
        bsub_bin=str(MOCK_BSUB),
        invoker=[sys.executable],
    )
    # Second call — upsert returns False; existing SUBMITTED row stays as is.
    lsf_run_client.dispatch(
        idempotency_key=idem,
        asset_name="a", partition_key="p",
        inner_argv=["true"],
        fabric_worker_path="/abs/fw.py",
        db_path=str(db),
        lsf_cfg=cfg,
        log_dir=str(tmp_path / "logs2"),
        bsub_bin=str(MOCK_BSUB),
        invoker=[sys.executable],
    )
    # Still only one row; state is SUBMITTED with a job_id from one of the two bsubs.
    import sqlite3
    with sqlite3.connect(db) as conn:
        (n,) = conn.execute("SELECT COUNT(*) FROM tasks WHERE idempotency_key=?",
                            (idem,)).fetchone()
    assert n == 1
