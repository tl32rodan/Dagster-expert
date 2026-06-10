#!/usr/bin/env python3
"""Mock LSF `bjobs` for local development of LSFRunLauncher's
check_run_worker_health path (lesson 13 Part B).

Real bjobs invocation (from LSFRunLauncher.check_run_worker_health):
    bjobs -a -o "stat exit_code" -noheader <job_id>

Mock behavior (sync model):
- Mock `bsub` runs synchronously (-K), so any job bsub returned for is
  by definition DONE. The mock cannot distinguish PEND/RUN/EXIT —
  those LSF states only manifest on a real grid.
- For any single job_id argument, print "DONE 0" to stdout.
- For -p / -r filters (pending/running only), print empty (correct
  given the sync mock model).
- For -u <user>, treat as a filter, print empty unless a specific
  job_id is also given.
- Real LSF state-machine behavior (PEND/RUN/EXIT/DONE transitions) is
  exercised on the real grid; the launcher's WorkerStatus mapping (see
  `pipelines/launcher.py:check_run_worker_health`) covers all five.

Drop on PATH before `dagster dev` or in `_smoke_launcher.py`:
    export PATH=$(dirname $(realpath $0)):$PATH
"""

import sys


def main() -> int:
    argv = sys.argv[1:]
    pending_only = False
    running_only = False
    job_id = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "-p":
            pending_only = True
            i += 1
        elif a == "-r":
            running_only = True
            i += 1
        elif a in ("-a", "-noheader", "-w", "-l"):
            i += 1
        elif a in ("-o", "-u"):
            i += 2
        elif a.startswith("-"):
            i += 1
        else:
            job_id = a
            i += 1

    if pending_only or running_only:
        # Filters that exclude DONE — mock has no in-flight jobs to report.
        return 0

    if job_id:
        # Sync mock: bsub completed = DONE with exit 0.
        print("DONE 0")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
