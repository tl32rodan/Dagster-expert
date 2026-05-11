# all-might generated
#!/usr/bin/env python3
"""All-Might memory-history hook — Claude Code mirror of memory-history.ts.

Stop hook: spawns ``allmight memory snapshot`` after every agent
turn. Backs accidental-delete recovery via ``allmight memory
restore``. Errors are swallowed; the hook must never block.

The OpenCode sibling is ``.opencode/plugins/memory-history.ts``;
both surfaces call the same CLI so behaviour is identical.
"""
import json
import os
import subprocess
import sys


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        payload = {}

    cwd = payload.get("cwd") or os.getcwd()
    sid = (payload.get("session_id") or "")[:32]

    args = ["allmight", "memory", "snapshot", "--trigger=stop-hook"]
    if sid:
        args.append(f"--session-id={sid}")

    try:
        subprocess.Popen(
            args,
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
    except (FileNotFoundError, OSError):
        # allmight not on PATH or spawn failed — silent. Recovery via
        # `allmight memory snapshot` by hand still works.
        pass

    # Empty output means: don't block, no extra context to inject.
    print("{}")


if __name__ == "__main__":
    main()
