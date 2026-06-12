#!/usr/bin/env python3
"""liberate_char fabric_worker — runs ON the LSF node (WHITEPAPER §3.5).

Invoked by `lsf_run_client.dispatch` via the bsub wrapper. CLI:

  fabric_worker.py --db-path <p> --idempotency-key <k> \
                   --asset-name <a> --partition-key <p> \
                   -- <inner argv...>

Responsibilities:
  1. subprocess.run(inner_argv, check=True)        # set -e equivalent
  2. data_version = _compute_data_version_from_ldb(args)
  3. status_db.mark_success(...)  under file_lock
  4. on any exception: mark_failed + re-raise (for LSF post-exec hooks)

Lives WITH the flow (not the framework) because data_version derivation
is flow-specific. The framework provides only the boilerplate via
`framework/fabric/_worker_main.py`'s helpers — see WHITEPAPER §5.4.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# Ensure execution_fabric/ is on sys.path so `framework.fabric` imports work
# when this script is invoked by its absolute path (LSF node has no CWD
# guarantee).
_ROOT = Path(__file__).resolve().parents[3]   # …/execution_fabric
sys.path.insert(0, str(_ROOT))

from framework.fabric import file_lock, status_db  # noqa: E402


def _compute_data_version_from_ldb(out_dir: Path, pvt: str, cell: str) -> str:
    """Read the `.ldb` digest line. Falls back to file-mtime + size if
    digest line is absent (older mock liberate output)."""
    ldb = out_dir / f"{pvt}__{cell}.ldb"
    if not ldb.exists():
        raise FileNotFoundError(f".ldb output missing: {ldb}")
    for line in ldb.read_text().splitlines():
        if line.startswith("digest "):
            return line.split()[1]
    st = ldb.stat()
    return f"sz{st.st_size}_mt{int(st.st_mtime)}"


def _extract_pvt_cell(inner_argv: list[str]) -> tuple[str, str, Path]:
    pvt = cell = None
    out_dir = None
    for i, a in enumerate(inner_argv):
        if a == "--pvt":
            pvt = inner_argv[i + 1]
        elif a == "--cell":
            cell = inner_argv[i + 1]
        elif a == "--out-dir":
            out_dir = Path(inner_argv[i + 1])
    if not (pvt and cell and out_dir):
        raise ValueError("inner argv missing --pvt/--cell/--out-dir")
    return pvt, cell, out_dir


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--idempotency-key", required=True)
    ap.add_argument("--asset-name", required=True)
    ap.add_argument("--partition-key", default="")
    args, rest = ap.parse_known_args()

    # rest = ['--', *inner_argv]; strip the separator
    if rest and rest[0] == "--":
        rest = rest[1:]
    inner_argv = rest

    try:
        proc = subprocess.run(
            inner_argv, capture_output=True, text=True, env=os.environ.copy()
        )
        sys.stdout.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, inner_argv)

        pvt, cell, out_dir = _extract_pvt_cell(inner_argv)
        data_version = _compute_data_version_from_ldb(out_dir, pvt, cell)
        with file_lock.with_write_lock(args.db_path):
            status_db.mark_success(args.db_path, args.idempotency_key, data_version)
        return 0

    except Exception as e:
        try:
            with file_lock.with_write_lock(args.db_path):
                status_db.mark_failed(args.db_path, args.idempotency_key, str(e))
        except Exception as inner_e:
            sys.stderr.write(f"[fabric_worker] mark_failed itself failed: {inner_e}\n")
        sys.stderr.write(f"[fabric_worker] failed: {e}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
