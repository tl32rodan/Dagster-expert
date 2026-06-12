"""Write-lock helper for SQLite status DB (WHITEPAPER §3.4 file_lock).

`fcntl.flock(LOCK_EX)` on a sibling lockfile serializes writes from
multiple processes/threads on the same host. Kernel releases the lock
if the holder dies (no orphan lockfile problem).

Phase 2 (PostgreSQL) drops this — Postgres serializes writes itself.
"""
from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def with_write_lock(db_path: Path | str):
    p = Path(db_path)
    lock_path = p.with_suffix(p.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
