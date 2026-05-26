#!/usr/bin/env python3
"""Mock LSF submit wrapper. Parses bsub-ish flags, then a trailing '--' and the
inner command, and submits via (mock) bsub -K (synchronous), forwarding the
environment so dagster-pipes vars reach the inner process.

This is the STANDARD_USAGE 8 pattern: cluster dispatch from inside the asset
body via PipesSubprocessClient -> bsub. We do NOT subclass RunLauncher and we
do NOT build a custom multi-thread launcher (wrong layer; see STANDARD_USAGE 9c).
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    argv = sys.argv[1:]
    if "--" in argv:
        i = argv.index("--")
        mine, inner = argv[:i], argv[i + 1:]
    else:
        mine, inner = argv, []

    ap = argparse.ArgumentParser()
    ap.add_argument("--job-name", default="job")
    ap.add_argument("--queue", default="normal")
    ap.add_argument("--walltime", default="00:30")
    ap.add_argument("--memory-mb", default="2048")
    ap.add_argument("--log-dir", default="/tmp")
    ap.add_argument("--max-pending", default="100")
    ap.add_argument("--env-mode", default="pipes-only")
    a = ap.parse_args(mine)

    bsub = Path(__file__).resolve().parent / "bin" / "bsub"
    cmd = [sys.executable, str(bsub), "-K", "-q", a.queue, "-J", a.job_name, "--"] + inner
    # forward the full env so DAGSTER_PIPES_* reach the inner process
    return subprocess.run(cmd, env=os.environ.copy()).returncode


if __name__ == "__main__":
    sys.exit(main())
