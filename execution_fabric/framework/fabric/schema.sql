-- Status DB schema (WHITEPAPER §3.4, §10.1).
-- Phase 1: SQLite. Phase 2: identical DDL ports to PostgreSQL.
-- Phase-2 columns (worker_id, lease_expires) are present but NULL in Phase 1
-- so the upgrade is data-only, not schema-altering.

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
