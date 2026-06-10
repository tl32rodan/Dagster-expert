#!/usr/bin/env python3
"""Mock LSF `bkill` for local development (lesson 13 Part B).

Real LSF: terminates a job by id; LSFRunLauncher.terminate() calls this.

Mock: noop + echo. In the sync mock model, by the time terminate()
is invoked the job has already DONE (mock bsub -K is synchronous),
so bkill is harmless. The launcher.terminate() contract only requires
that bkill exits 0 and that we can later detect via bjobs whether the
job actually died — both true here.
"""

import sys


def main() -> int:
    argv = sys.argv[1:]
    for a in argv:
        if not a.startswith("-") and a.isdigit():
            print(f"Job <{a}> is being terminated", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
