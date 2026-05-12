#!/usr/bin/env python3
"""Generic mock python step. scripts/python/<step>.py are symlinks to
this file. Real production scripts replace each symlink with an
implementation (potentially using dagster_pipes for Tier-2 lineage; see
lesson 09 ``liberate_invoke.py``).
"""
import argparse
from pathlib import Path
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--library", required=True)
    ap.add_argument("--branch", required=True)
    ap.add_argument("--step", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    summary = out / "result.txt"
    summary.write_text(
        f"library={args.library}\n"
        f"branch={args.branch}\n"
        f"step={args.step}\n"
        f"runner=python\n"
        f"timestamp={int(time.time())}\n"
    )
    blob = out / "blob.bin"
    payload = f"{args.library}|{args.branch}|{args.step}".encode()
    blob.write_bytes(payload * 10)


if __name__ == "__main__":
    main()
