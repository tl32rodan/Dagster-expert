"""Status DB API (WHITEPAPER §3.4).

**Production storage is PostgreSQL.** This module is the reference
adapter, backed by SQLite, used by the in-repo demo + tests on a single
host. Production deployments swap in a psycopg2-backed module exposing
the same public surface (the function names below ARE the contract).

`schema.sql` is the Postgres-compatible DDL; the only port-time edit
is `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGSERIAL PRIMARY KEY`.

Why not SQLite + file lock in production: WHITEPAPER §11.1 (rejected
designs). NFS does not honor `fcntl.flock` reliably; SQLite on NFS
corrupts under concurrent writes. Postgres serializes writes itself.

For the single-host demo, `init_db` enables WAL + a generous busy
timeout. That is sufficient for one orchestrator + one mock LSF host
writing concurrently; it does NOT make this safe for NFS.
"""
from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_SCHEMA = (Path(__file__).parent / "schema.sql").read_text()

STATE_PENDING = "PENDING"
STATE_SUBMITTED = "SUBMITTED"
STATE_RUNNING = "RUNNING"
STATE_SUCCESS = "SUCCESS"
STATE_FAILED = "FAILED"
TERMINAL_STATES = (STATE_SUCCESS, STATE_FAILED)


@dataclass
class TaskRow:
    id: int
    idempotency_key: str
    asset_name: str
    partition_key: str | None
    state: str
    lsf_job_id: str | None
    data_version: str | None
    error_message: str | None
    harvested: int


def init_db(db_path: Path | str) -> None:
    """Create DB file + apply schema. Idempotent.

    Enables WAL mode + 5s busy timeout so single-host multi-writer
    (orchestrator + fabric_worker on the same node) does not need
    fcntl-level serialization.
    """
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(p) as conn:
        conn.executescript(_SCHEMA)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")


def compute_idempotency_key(
    asset_name: str,
    partition_key: str | None,
    upstream_data_versions: Iterable[str],
) -> str:
    """sha256(asset|partition|sorted(upstream dvs)) — WHITEPAPER §7.1."""
    parts = [asset_name, partition_key or ""]
    parts.extend(sorted(upstream_data_versions))
    blob = "|".join(parts).encode()
    return hashlib.sha256(blob).hexdigest()


def upsert_pending(
    db_path: Path | str,
    idempotency_key: str,
    asset_name: str,
    partition_key: str | None,
) -> bool:
    """INSERT a PENDING row. Returns True if inserted, False if absorbed by UNIQUE.

    The UNIQUE constraint enforces §7.1 "no double dispatch for unchanged
    idempotency_key". Production Postgres adapters may add active-state-aware
    conflict resolution (INSERT … ON CONFLICT DO NOTHING).
    """
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute(
                "INSERT INTO tasks(idempotency_key, asset_name, partition_key, "
                "state, submitted_at) VALUES (?, ?, ?, ?, ?)",
                (idempotency_key, asset_name, partition_key, STATE_PENDING, int(time.time())),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def mark_submitted(db_path: Path | str, idempotency_key: str, lsf_job_id: str | None) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE tasks SET state=?, lsf_job_id=?, submitted_at=? "
            "WHERE idempotency_key=? AND state=?",
            (STATE_SUBMITTED, lsf_job_id, int(time.time()), idempotency_key, STATE_PENDING),
        )


def mark_running(db_path: Path | str, idempotency_key: str) -> None:
    """Production: the bjobs synchronizer calls this on PEND → RUN.
    Unused by the in-repo reference/demo (SUBMITTED conflates RUNNING)."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE tasks SET state=?, running_at=? WHERE idempotency_key=?",
            (STATE_RUNNING, int(time.time()), idempotency_key),
        )


def mark_success(db_path: Path | str, idempotency_key: str, data_version: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE tasks SET state=?, data_version=?, terminal_at=? "
            "WHERE idempotency_key=?",
            (STATE_SUCCESS, data_version, int(time.time()), idempotency_key),
        )


def mark_failed(db_path: Path | str, idempotency_key: str, error_message: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE tasks SET state=?, error_message=?, terminal_at=? "
            "WHERE idempotency_key=?",
            (STATE_FAILED, error_message[:1000], int(time.time()), idempotency_key),
        )


def list_successful_partitions(db_path: Path | str, asset_name: str) -> set[str]:
    """Dispatch sensor's `observed` set (WHITEPAPER §3.3).

    Comes from status DB, NOT Dagster materializations — see whitepaper
    §3.3 rationale + R7 placeholder-materialization hazard.
    """
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT partition_key FROM tasks "
            "WHERE asset_name=? AND state=? AND partition_key IS NOT NULL",
            (asset_name, STATE_SUCCESS),
        ).fetchall()
    return {r[0] for r in rows}


def list_unharvested_terminals(
    db_path: Path | str, after_id: int = 0, limit: int = 200
) -> list[TaskRow]:
    """Harvest sensor input. Stable order by id (cursor advances by max id)."""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, idempotency_key, asset_name, partition_key, state, "
            "       lsf_job_id, data_version, error_message, harvested "
            "FROM tasks "
            "WHERE harvested=0 AND state IN (?, ?) AND id > ? "
            "ORDER BY id ASC LIMIT ?",
            (STATE_SUCCESS, STATE_FAILED, after_id, limit),
        ).fetchall()
    return [TaskRow(*r) for r in rows]


def mark_harvested(db_path: Path | str, ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"UPDATE tasks SET harvested=1 WHERE id IN ({placeholders})", ids)


def count_in_state(db_path: Path | str, asset_name: str, state: str) -> int:
    with sqlite3.connect(db_path) as conn:
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE asset_name=? AND state=?",
            (asset_name, state),
        ).fetchone()
    return n


def get_task(db_path: Path | str, idempotency_key: str) -> TaskRow | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, idempotency_key, asset_name, partition_key, state, "
            "       lsf_job_id, data_version, error_message, harvested "
            "FROM tasks WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
    return TaskRow(*row) if row else None
