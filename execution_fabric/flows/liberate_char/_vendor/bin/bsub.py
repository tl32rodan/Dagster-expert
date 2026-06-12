#!/usr/bin/env python3
"""Mock `bsub` — non-blocking semantics (WHITEPAPER §3.4).

Real LSF `bsub` (without -K) writes "Job <N> is submitted to queue <Q>."
on stderr and returns immediately while the actual job runs detached on
an LSF node.

This mock:
  - parses standard bsub flags (loosely)
  - prints the standard "Job <N> is submitted to queue <Q>." line
  - **forks** the post-`--` command in a new session, inheriting env
    (so DAGSTER_HOME, FABRIC_STATUS_DB, LIBERATE_DAG_ROOT, etc. pass)
  - returns 0 immediately (does NOT wait for the child)

On a real LSF host the real bsub takes over; this mock disappears.
"""
import os
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    argv = sys.argv[1:]
    queue = "normal"
    out_file = None
    err_file = None
    i = 0
    inner: list[str] = []
    while i < len(argv):
        a = argv[i]
        if a == "--":
            inner = argv[i + 1:]
            break
        if a == "-q":
            queue = argv[i + 1]; i += 2
        elif a in ("-J", "-n", "-W", "-R", "-P", "-env"):
            i += 2
        elif a == "-o":
            out_file = argv[i + 1]; i += 2
        elif a == "-e":
            err_file = argv[i + 1]; i += 2
        elif a.startswith("-"):
            i += 1
        else:
            # First non-flag without preceding `--` is the start of the inner command
            inner = argv[i:]
            break

    job_id = int(time.time_ns() / 1000) % 1_000_000
    sys.stderr.write(f"Job <{job_id}> is submitted to queue <{queue}>.\n")

    if not inner:
        return 0

    out_fd = open(out_file, "w") if out_file else subprocess.DEVNULL
    err_fd = open(err_file, "w") if err_file else subprocess.DEVNULL
    try:
        subprocess.Popen(
            inner,
            env=os.environ.copy(),
            stdout=out_fd,
            stderr=err_fd,
            start_new_session=True,  # detach so we can exit without waiting
        )
    finally:
        if out_file:
            out_fd.close()
        if err_file:
            err_fd.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
