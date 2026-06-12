"""File-lock concurrent-write safety (WHITEPAPER §3.4 file_lock).

fcntl.flock serializes; orphaned lockfile from a dead holder doesn't
keep the lock alive (kernel tracks ownership by FD).
"""
import multiprocessing as mp
import time
from pathlib import Path

from framework.fabric import file_lock, status_db


def _writer_worker(db_path, key_prefix, n):
    for i in range(n):
        key = status_db.compute_idempotency_key(f"asset_{key_prefix}", str(i), [])
        with file_lock.with_write_lock(db_path):
            status_db.upsert_pending(db_path, key, f"asset_{key_prefix}", str(i))


def test_concurrent_writes_serialize(tmp_path):
    db = tmp_path / "fabric.db"
    status_db.init_db(db)
    # 4 writers × 25 rows each — must all land without corruption.
    procs = [
        mp.Process(target=_writer_worker, args=(str(db), str(i), 25))
        for i in range(4)
    ]
    for p in procs: p.start()
    for p in procs: p.join(timeout=15)
    for p in procs: assert p.exitcode == 0, f"writer exit={p.exitcode}"

    import sqlite3
    with sqlite3.connect(db) as conn:
        (count,) = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()
    assert count == 100, f"expected 100 rows, got {count}"


def test_lock_released_on_exception(tmp_path):
    """Context manager releases lock even when body raises."""
    db = tmp_path / "fabric.db"
    status_db.init_db(db)
    try:
        with file_lock.with_write_lock(db):
            raise RuntimeError("oops")
    except RuntimeError:
        pass
    # Second acquire must not block.
    start = time.time()
    with file_lock.with_write_lock(db):
        pass
    assert time.time() - start < 1.0


def test_lockfile_lives_next_to_db(tmp_path):
    db = tmp_path / "fabric.db"
    status_db.init_db(db)
    with file_lock.with_write_lock(db):
        pass
    assert (tmp_path / "fabric.db.lock").exists()
