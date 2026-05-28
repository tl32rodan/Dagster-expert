#!/usr/bin/env python3
"""Mock `bsub` for local/air-gap dev. `-K` = synchronous: run the command after
'--' inline (inheriting the environment so dagster-pipes vars pass through) and
return its exit code. On a real LSF host the real bsub is on PATH instead; the
asset code does not change."""
import os
import subprocess
import sys


def main() -> int:
    argv = sys.argv[1:]
    inner = argv[argv.index("--") + 1:] if "--" in argv else argv
    sys.stderr.write(f"bsub: submitting <{' '.join(inner[:2])} ...> (mock, -K synchronous)\n")
    return subprocess.run(inner, env=os.environ.copy()).returncode


if __name__ == "__main__":
    sys.exit(main())
