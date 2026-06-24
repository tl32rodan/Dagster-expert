-- Status DB schema (WHITEPAPER §3.4).
--
-- Production = PostgreSQL. This file ships as Postgres-compatible DDL.
-- For the SQLite reference adapter, AUTOINCREMENT is honored as-is;
-- when porting to Postgres, change `INTEGER PRIMARY KEY AUTOINCREMENT`
-- to `BIGSERIAL PRIMARY KEY`. The remaining columns + indexes are
-- portable verbatim.
--
-- Columns reserved for production fault-recovery (worker_id, lease_expires)
-- are NULL in the single-host reference; their use is documented in
-- WHITEPAPER §6.

CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT    NOT NULL UNIQUE,
    asset_name      TEXT    NOT NULL,
    partition_key   TEXT,
    state           TEXT    NOT NULL,
    lsf_job_id      TEXT,
    worker_id       TEXT,
    lease_expires   INTEGER,
    data_version    TEXT,
    error_message   TEXT,
    submitted_at    INTEGER,
    running_at      INTEGER,
    terminal_at     INTEGER,
    harvested       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_tasks_state_id   ON tasks(state, id);
CREATE INDEX IF NOT EXISTS ix_tasks_harvested  ON tasks(harvested);
CREATE INDEX IF NOT EXISTS ix_tasks_asset_part ON tasks(asset_name, partition_key);
