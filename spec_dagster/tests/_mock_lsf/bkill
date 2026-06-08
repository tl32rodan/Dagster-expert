#!/usr/bin/env python3
"""Mock bkill. Drops a marker file the test can assert on."""
import os
import sys
from pathlib import Path


def main() -> int:
    for a in sys.argv[1:]:
        if not a.startswith("-") and a.isdigit():
            killed = os.environ.get("MOCK_LSF_KILLED")
            if killed:
                Path(killed).parent.mkdir(parents=True, exist_ok=True)
                with open(killed, "a") as f:
                    f.write(a + "\n")
            sys.stderr.write(f"Job <{a}> is being terminated\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
