"""Non-blocking LSF client (WHITEPAPER §3.4 lsf_run_client).

dispatch() does TWO things and returns. NO `-K`, NO wait.
  1. INSERT a PENDING row in status DB (UNIQUE absorbs dupes per §6.1).
  2. bsub the fabric_worker wrapper; parse job_id from bsub output;
     mark SUBMITTED.

The fabric_worker on the LSF node is what actually runs the computation,
self-computes the data_version, and writes SUCCESS back to status DB.

Phase 2 adds: priority queue between sensor and dispatch (Kafka);
synchronizer thread for bjobs SUBMITTED→RUNNING transitions; reaper for
orphan recovery. None of that is in this module.
"""
from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from framework.fabric import file_lock, status_db


_BSUB_JOB_ID_RE = re.compile(r"Job\s+<?(\d+)>?\s+is submitted", re.IGNORECASE)


@dataclass
class LSFConfig:
    """Per-asset LSF resource spec, read from spec.yaml asset.lsf / spec.lsf."""
    queue: str = "normal"
    cores: int = 4
    mem_mb: int = 4096
    walltime: str = "24:00"
    project: str | None = None


def _parse_job_id(bsub_output: str) -> str | None:
    m = _BSUB_JOB_ID_RE.search(bsub_output)
    return m.group(1) if m else None


def _build_bsub_argv(
    idempotency_key: str,
    inner_argv: list[str],
    fabric_worker_path: Path | str,
    db_path: Path | str,
    asset_name: str,
    partition_key: str | None,
    lsf_cfg: LSFConfig,
    log_dir: Path | str,
    bsub_bin: str = "bsub",
    invoker: list[str] | None = None,
) -> list[str]:
    """Build the bsub command. NO -K (non-blocking). See WHITEPAPER §3.4.

    `invoker` lets tests inject `[sys.executable]` to run a `.py` mock
    bsub without relying on shebang + PATH.
    """
    argv = list(invoker or [])
    argv.append(bsub_bin)
    argv += ["-J", f"fabric_{idempotency_key[:8]}"]
    argv += ["-q", lsf_cfg.queue, "-n", str(lsf_cfg.cores)]
    argv += ["-R", f"rusage[mem={lsf_cfg.mem_mb}]", "-W", lsf_cfg.walltime]
    if lsf_cfg.project:
        argv += ["-P", lsf_cfg.project]
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    argv += ["-o", str(log_dir / f"{idempotency_key}.out")]
    argv += ["-e", str(log_dir / f"{idempotency_key}.err")]
    argv += ["-env", "DAGSTER_HOME,PATH,PYTHONPATH,LIBERATE_DAG_ROOT"]
    argv += [
        sys.executable, str(fabric_worker_path),
        "--db-path", str(db_path),
        "--idempotency-key", idempotency_key,
        "--asset-name", asset_name,
        "--partition-key", partition_key or "",
        "--",
    ]
    argv += list(inner_argv)
    return argv


def dispatch(
    *,
    idempotency_key: str,
    asset_name: str,
    partition_key: str | None,
    inner_argv: list[str],
    fabric_worker_path: Path | str,
    db_path: Path | str,
    lsf_cfg: LSFConfig,
    log_dir: Path | str,
    bsub_bin: str = "bsub",
    invoker: list[str] | None = None,
) -> str | None:
    """Non-blocking dispatch. Returns the LSF job id (str) or None on parse miss.

    Raises subprocess.CalledProcessError if bsub itself fails.
    """
    with file_lock.with_write_lock(db_path):
        status_db.upsert_pending(db_path, idempotency_key, asset_name, partition_key)

    bsub_argv = _build_bsub_argv(
        idempotency_key=idempotency_key,
        inner_argv=inner_argv,
        fabric_worker_path=fabric_worker_path,
        db_path=db_path,
        asset_name=asset_name,
        partition_key=partition_key,
        lsf_cfg=lsf_cfg,
        log_dir=log_dir,
        bsub_bin=bsub_bin,
        invoker=invoker,
    )
    proc = subprocess.run(bsub_argv, capture_output=True, text=True, check=True)
    job_id = _parse_job_id(proc.stdout + proc.stderr)

    with file_lock.with_write_lock(db_path):
        status_db.mark_submitted(db_path, idempotency_key, lsf_job_id=job_id)

    return job_id
