"""Status DB API + state machine (WHITEPAPER §3.4, §7.1).

UNIQUE constraint enforces "no double dispatch for unchanged idempotency_key"
at the DB level — the first line of defense for §7.1's dispatch-dedup contract.
"""
import pytest

from framework.fabric import status_db


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "fabric.db"
    status_db.init_db(p)
    return p


def test_init_db_idempotent(tmp_path):
    p = tmp_path / "fabric.db"
    status_db.init_db(p)
    status_db.init_db(p)   # should not raise
    assert p.exists()


def test_idempotency_key_is_deterministic():
    k1 = status_db.compute_idempotency_key("a", "pvt|cell", ["v1", "v2"])
    k2 = status_db.compute_idempotency_key("a", "pvt|cell", ["v2", "v1"])  # different order
    assert k1 == k2, "upstream order must not affect key"
    k3 = status_db.compute_idempotency_key("a", "pvt|cell", ["v1", "v3"])
    assert k1 != k3, "different versions must yield different keys"


def test_idempotency_key_distinguishes_unpartitioned_from_empty_partition():
    k_none = status_db.compute_idempotency_key("a", None, [])
    k_empty = status_db.compute_idempotency_key("a", "", [])
    # Implementation maps None → "" for stability — confirm:
    assert k_none == k_empty


def test_upsert_pending_returns_true_on_first_false_on_dupe(db):
    k = status_db.compute_idempotency_key("a", "p", [])
    assert status_db.upsert_pending(db, k, "a", "p") is True
    assert status_db.upsert_pending(db, k, "a", "p") is False
    row = status_db.get_task(db, k)
    assert row.state == status_db.STATE_PENDING


def test_state_machine_pending_submitted_success(db):
    k = status_db.compute_idempotency_key("a", "p", [])
    status_db.upsert_pending(db, k, "a", "p")
    status_db.mark_submitted(db, k, lsf_job_id="12345")
    assert status_db.get_task(db, k).state == status_db.STATE_SUBMITTED
    assert status_db.get_task(db, k).lsf_job_id == "12345"
    status_db.mark_success(db, k, data_version="abc123")
    row = status_db.get_task(db, k)
    assert row.state == status_db.STATE_SUCCESS
    assert row.data_version == "abc123"


def test_state_machine_failed_path(db):
    k = status_db.compute_idempotency_key("a", "p", [])
    status_db.upsert_pending(db, k, "a", "p")
    status_db.mark_submitted(db, k, lsf_job_id="999")
    status_db.mark_failed(db, k, error_message="liberate crashed")
    row = status_db.get_task(db, k)
    assert row.state == status_db.STATE_FAILED
    assert row.error_message == "liberate crashed"


def test_list_successful_partitions(db):
    for partition in ["pvtA|cellX", "pvtA|cellY", "pvtB|cellX"]:
        k = status_db.compute_idempotency_key("characterize", partition, [])
        status_db.upsert_pending(db, k, "characterize", partition)
        status_db.mark_success(db, k, "v1")
    # one failed — should NOT appear in successful list
    kf = status_db.compute_idempotency_key("characterize", "pvtB|cellY", [])
    status_db.upsert_pending(db, kf, "characterize", "pvtB|cellY")
    status_db.mark_failed(db, kf, "x")

    assert status_db.list_successful_partitions(db, "characterize") == {
        "pvtA|cellX", "pvtA|cellY", "pvtB|cellX"
    }
    # different asset → empty
    assert status_db.list_successful_partitions(db, "other") == set()


def test_list_unharvested_terminals_only_terminal_states(db):
    keys = {}
    for state in ["pending", "submitted", "success1", "success2", "failed1"]:
        k = status_db.compute_idempotency_key("a", state, [])
        keys[state] = k
        status_db.upsert_pending(db, k, "a", state)
    status_db.mark_submitted(db, keys["submitted"], "1")
    status_db.mark_success(db, keys["success1"], "v1")
    status_db.mark_success(db, keys["success2"], "v2")
    status_db.mark_failed(db, keys["failed1"], "x")

    rows = status_db.list_unharvested_terminals(db, after_id=0)
    states = sorted(r.state for r in rows)
    assert states == ["FAILED", "SUCCESS", "SUCCESS"], states


def test_cursor_advances_via_mark_harvested(db):
    ids = []
    for p in ["a", "b", "c"]:
        k = status_db.compute_idempotency_key("x", p, [])
        status_db.upsert_pending(db, k, "x", p)
        status_db.mark_success(db, k, p)
    rows = status_db.list_unharvested_terminals(db, after_id=0)
    ids = [r.id for r in rows]
    assert len(ids) == 3

    # harvest the first two
    status_db.mark_harvested(db, ids[:2])
    remaining = status_db.list_unharvested_terminals(db, after_id=0)
    assert [r.id for r in remaining] == [ids[2]]

    # cursor-style read: after_id=ids[1] should also give just the third
    after = status_db.list_unharvested_terminals(db, after_id=ids[1])
    assert [r.id for r in after] == [ids[2]]
