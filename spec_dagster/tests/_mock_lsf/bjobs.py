#!/usr/bin/env python3
"""Mock bjobs. State file model: $MOCK_LSF_STATE/<job_id> contains the
state line (e.g. 'DONE 0' or 'EXIT 1'); if absent, treat as DONE 0."""
import os
import sys
from pathlib import Path


def main() -> int:
    args = sys.argv[1:]
    # last positional is job id (everything after the last flag pair)
    job_id = None
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("-p", "-r"):
            return 0  # filters always empty
        if a in ("-o", "-u"):
            i += 2
        elif a in ("-a", "-noheader", "-w", "-l"):
            i += 1
        elif a.startswith("-"):
            i += 1
        else:
            job_id = a
            i += 1
    if not job_id:
        return 0
    state_dir = os.environ.get("MOCK_LSF_STATE")
    if state_dir:
        f = Path(state_dir) / job_id
        if f.exists():
            print(f.read_text().strip())
            return 0
    print("DONE 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
