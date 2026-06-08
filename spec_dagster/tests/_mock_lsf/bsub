#!/usr/bin/env python3
"""Mock LSF bsub for the framework LSFRunLauncher integration test.

Behavior:
  - Parses bsub flags loosely (whatever appears before --) and prints the
    standard 'Job <NNNN> is submitted to queue <Q>.' line on STDERR (real
    bsub writes this to stderr in 1.x).
  - Echoes the would-have-executed worker command to a sidecar file at
    $MOCK_LSF_RECORD/<run_id>.argv so the test can assert what the
    launcher tried to ship to LSF, WITHOUT actually executing
    `dagster api execute_run` (the worker side is out of scope for this
    launcher test).
  - Exits 0.

The integration test sets $MOCK_LSF_RECORD to a tmp dir and inspects the
files after calling launcher.launch_run().
"""
import os
import re
import sys
import time
import uuid
from pathlib import Path


def main() -> int:
    argv = sys.argv[1:]
    queue = "normal"
    out_file = None
    err_file = None
    # walk flags
    i = 0
    worker = []
    while i < len(argv):
        a = argv[i]
        if a == "-q":
            queue = argv[i + 1]
            i += 2
        elif a in ("-J", "-n", "-W", "-R", "-P", "-env"):
            i += 2
        elif a == "-o":
            out_file = argv[i + 1]
            i += 2
        elif a == "-e":
            err_file = argv[i + 1]
            i += 2
        elif a.startswith("-"):
            i += 1
        else:
            worker = argv[i:]
            break

    job_id = int(time.time_ns() / 1000) % 1_000_000
    sys.stderr.write(f"Job <{job_id}> is submitted to queue <{queue}>.\n")

    record_dir = os.environ.get("MOCK_LSF_RECORD")
    if record_dir:
        Path(record_dir).mkdir(parents=True, exist_ok=True)
        # try to identify the run id from the worker argv;
        # dagster api execute_run has a JSON-ish arg containing run_id
        run_id_guess = "unknown"
        joined = " ".join(worker)
        m = re.search(r'"run_id":\s*"([a-f0-9-]+)"', joined)
        if m:
            run_id_guess = m.group(1)
        else:
            run_id_guess = uuid.uuid4().hex[:8]
        rec = Path(record_dir) / f"{run_id_guess}.argv"
        rec.write_text(repr(argv))
        Path(record_dir, f"{job_id}.jobid").write_text(run_id_guess)
    return 0


if __name__ == "__main__":
    sys.exit(main())
