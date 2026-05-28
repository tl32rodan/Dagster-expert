#!/usr/bin/env python3
"""Undo the transport-safe extension encoding of this branch.

This branch (claude/transport-safe-AER0D) renamed every file whose
extension is not on a typical corporate text allowlist (e.g. .tcl, .sp,
.scr, .csh, .sh, .pl, .log, .gitkeep, and extensionless tool scripts) by
appending ".txt", so the downloaded ZIP/tarball contains only common
text extensions. The content of each file is unchanged.

Run this ONCE from the repo root after you have downloaded and unpacked
the archive on the target box, to restore the original filenames:

    python restore_transport.py        # (python3)

It reads _TRANSPORT_MANIFEST.txt (one "<renamed><TAB><original>" per
line) and renames each file back. After it finishes you can delete this
script and the manifest.
"""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
MANIFEST = os.path.join(ROOT, "_TRANSPORT_MANIFEST.txt")


def main() -> int:
    if not os.path.exists(MANIFEST):
        print(f"manifest not found: {MANIFEST}")
        return 1
    restored = missing = 0
    with open(MANIFEST, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or "\t" not in line:
                continue
            renamed, original = line.split("\t", 1)
            src = os.path.join(ROOT, renamed)
            dst = os.path.join(ROOT, original)
            if not os.path.exists(src):
                missing += 1
                continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            os.rename(src, dst)
            restored += 1
    print(f"restored {restored} file(s); {missing} already in place / missing")
    print("you may now delete restore_transport.py and _TRANSPORT_MANIFEST.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
