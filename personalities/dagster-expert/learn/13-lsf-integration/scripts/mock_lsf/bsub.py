#!/usr/bin/env python3
"""Mock LSF `bsub` for local development.

Behavior:
- Parses common bsub flags (-K, -J, -q, -o, -e, -W, -env, etc.)
- Prints submission line ("Job <NNNN> is submitted to queue ...")
- If -K (synchronous), exec's the inner command directly + returns
  its exit code (so caller's wait works the same as real bsub -K)
- If async, fork + run, prints jobid, returns 0

Drop on PATH before real `bsub`:
    export PATH=$(dirname $(realpath $0)):$PATH
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def parse_bsub_flags(argv: list[str]) -> tuple[dict, list[str]]:
    """Return (flag_dict, remaining_cmd)."""
    flags = {
        "K": False, "I": False,
        "J": None, "q": "normal", "P": None,
        "o": None, "e": None,
        "W": None, "n": None, "R": [],
        "cwd": None, "env": None,
    }
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-K", "-I", "-Ip", "-Is"):
            flags[a[1:]] = True
            i += 1
        elif a in ("-J", "-q", "-P", "-o", "-e", "-W", "-n", "-cwd", "-env"):
            flags[a[1:]] = argv[i + 1]
            i += 2
        elif a == "-R":
            flags["R"].append(argv[i + 1])
            i += 2
        elif a.startswith("-"):
            # Unknown flag — skip (real bsub would error; we're lenient)
            i += 1
        else:
            break
    return flags, argv[i:]


def main() -> int:
    flags, cmd = parse_bsub_flags(sys.argv[1:])
    if not cmd:
        print("mock bsub: no command", file=sys.stderr)
        return 1

    jobid = int(time.time() * 1000) % 1_000_000
    print(
        f"Job <{jobid}> is submitted to queue <{flags['q']}>.",
        file=sys.stderr,
    )

    # Build subprocess env per -env flag
    env = os.environ.copy()
    if flags["env"] == "all":
        pass  # inherit everything — real bsub does this
    elif flags["env"]:
        # parse "VAR1=val1, VAR2=val2"
        env = {}
        for piece in flags["env"].split(","):
            if "=" in piece:
                k, v = piece.strip().split("=", 1)
                env[k] = v

    cwd = flags["cwd"] if flags["cwd"] else None

    if flags["K"]:
        # Synchronous: run inline, return inner exit code (like bsub -K)
        # Redirect to log files if specified
        stdout = open(flags["o"], "w") if flags["o"] else None
        stderr = open(flags["e"], "w") if flags["e"] else None
        try:
            r = subprocess.run(cmd, env=env, cwd=cwd,
                                stdout=stdout, stderr=stderr)
            return r.returncode
        finally:
            if stdout: stdout.close()
            if stderr: stderr.close()
    else:
        # Async: fork + return immediately. (We don't write a real
        # job-state DB; -K is what real Dagster integration uses.)
        print(f"mock bsub: async mode not fully simulated; running inline",
              file=sys.stderr)
        stdout = open(flags["o"], "w") if flags["o"] else None
        stderr = open(flags["e"], "w") if flags["e"] else None
        subprocess.run(cmd, env=env, cwd=cwd,
                        stdout=stdout, stderr=stderr)
        if stdout: stdout.close()
        if stderr: stderr.close()
        return 0


if __name__ == "__main__":
    sys.exit(main())
