#!/usr/bin/env python3
# all-might generated — DO NOT EDIT.
#
# Mirror of .opencode/plugins/role-load.ts. Changes here MUST land in
# the .ts plugin too; see All-Might CLAUDE.md -> Editor Compatibility.
"""Role-load hook for Claude Code (SessionStart, PreCompact).

Reads every ``personalities/*/ROLE.md`` and emits the concatenated
content as ``additionalContext`` so the agent has each role primed
before the first user turn — same role-stability guarantee the
OpenCode role-load plugin gives via ``chat.message`` injection.
"""
import json
import os
import sys
from pathlib import Path


def main() -> int:
    cwd = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())
    parts: list[str] = []
    personalities_dir = cwd / "personalities"
    if personalities_dir.is_dir():
        for entry in sorted(personalities_dir.iterdir()):
            role = entry / "ROLE.md"
            if not role.is_file():
                continue
            try:
                body = role.read_text(encoding="utf-8")
            except OSError:
                continue
            parts.append(f"--- Role: {entry.name} (ROLE.md) ---")
            parts.append(body.rstrip())
            parts.append(f"--- End Role: {entry.name} ---")
            parts.append("")
    text = "\n".join(parts).strip()
    if not text:
        return 0

    try:
        payload = json.load(sys.stdin) if not sys.stdin.isatty() else {}
    except (json.JSONDecodeError, ValueError):
        payload = {}
    event = payload.get("hook_event_name") or "SessionStart"

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": event,
            "additionalContext": text,
        }
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
