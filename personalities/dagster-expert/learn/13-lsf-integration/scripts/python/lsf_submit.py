"""lsf_submit.py — reusable LSF wrapper for Dagster asset bodies.

Assembles bsub flags, forwards Pipes env, captures logs.
Designed to be invoked from PipesSubprocessClient.run([...]).

Usage:
    python lsf_submit.py \\
        --job-name <name> \\
        --queue <queue> \\
        --walltime hh:mm \\
        --memory-mb <int> \\
        --log-dir /shared/logs \\
        --max-pending 100 \\
        -- \\
        python3 wrapper.py --arg value

Everything after `--` is the inner command bsub will run.

For Dagster Pipes integration: Dagster sets DAGSTER_PIPES_CONTEXT
and DAGSTER_PIPES_MESSAGES in the env of THIS wrapper script.
We forward them via `bsub -env` so the inner wrapper can call
dagster_pipes.open_dagster_pipes().
"""

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path


def _bjobs_pending_count() -> int:
    """Return user's current LSF pending count, or -1 if bjobs unavailable."""
    try:
        r = subprocess.run(
            ["bjobs", "-u", os.environ.get("USER", "me"), "-p", "-noheader"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0:
            return -1
        return sum(1 for line in r.stdout.splitlines() if line.strip())
    except Exception:
        return -1


def _wait_for_queue_slot(max_pending: int, poll: int = 30):
    while True:
        n = _bjobs_pending_count()
        if n < 0:
            # bjobs unavailable (no LSF, or first call) — skip throttle
            return
        if n < max_pending:
            return
        print(f"lsf_submit: {n} pending >= {max_pending}, sleeping {poll}s",
              file=sys.stderr)
        time.sleep(poll)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--job-name", required=True)
    p.add_argument("--queue", default="normal")
    p.add_argument("--walltime", default="01:00",
                   help="LSF -W flag, format hh:mm")
    p.add_argument("--memory-mb", type=int, default=4096)
    p.add_argument("--slots", type=int, default=1)
    p.add_argument("--project", default=None)
    p.add_argument("--log-dir", default="/tmp/dagster-13-logs")
    p.add_argument("--max-pending", type=int, default=100,
                   help="Throttle bsub when user's LSF pending count exceeds this")
    p.add_argument("--env-mode", choices=["all", "pipes-only", "explicit"],
                   default="pipes-only",
                   help="What env vars to pass: all=everything, "
                        "pipes-only=only DAGSTER_PIPES_*, "
                        "explicit=use --env flag")
    p.add_argument("--env", action="append", default=[],
                   help="Explicit env (KEY=VAL); repeatable")
    p.add_argument("--async", dest="async_mode", action="store_true",
                   help="Use async bsub (no -K). Caller must poll bjobs.")
    p.add_argument("cmd", nargs=argparse.REMAINDER,
                   help="Inner command to bsub (everything after --)")
    args = p.parse_args()

    # Strip leading '--' from cmd if present
    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print("lsf_submit: no command after --", file=sys.stderr)
        return 1

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    job_id_tag = re.sub(r"[^A-Za-z0-9_.-]", "_", args.job_name)
    log_path = log_dir / f"{job_id_tag}.out"
    err_path = log_dir / f"{job_id_tag}.err"

    # Throttle (skip if no LSF)
    if not args.async_mode:
        _wait_for_queue_slot(args.max_pending)

    # Assemble bsub flags
    bsub_cmd = ["bsub"]
    if not args.async_mode:
        bsub_cmd.append("-K")
    bsub_cmd.extend([
        "-J", args.job_name,
        "-q", args.queue,
        "-o", str(log_path),
        "-e", str(err_path),
        "-W", args.walltime,
        "-n", str(args.slots),
        "-R", f"rusage[mem={args.memory_mb}]",
    ])
    if args.project:
        bsub_cmd.extend(["-P", args.project])

    # Env handling
    if args.env_mode == "all":
        bsub_cmd.extend(["-env", "all"])
    elif args.env_mode == "pipes-only":
        pipes_vars = ["DAGSTER_PIPES_CONTEXT", "DAGSTER_PIPES_MESSAGES",
                       "DAGSTER_HOME"]
        env_pairs = []
        for k in pipes_vars:
            v = os.environ.get(k)
            if v:
                env_pairs.append(f"{k}={v}")
        if env_pairs:
            bsub_cmd.extend(["-env", ",".join(env_pairs)])
    elif args.env_mode == "explicit":
        if args.env:
            bsub_cmd.extend(["-env", ",".join(args.env)])

    bsub_cmd.extend(cmd)

    print(f"lsf_submit: {' '.join(bsub_cmd)}", file=sys.stderr)
    result = subprocess.run(bsub_cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
