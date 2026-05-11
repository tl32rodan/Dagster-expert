#!/usr/bin/env python3
# all-might generated — DO NOT EDIT.
#
# Mirror of .opencode/plugins/memory-load.ts. Changes here MUST land in
# the .ts plugin too; see All-Might CLAUDE.md -> Editor Compatibility.
"""Memory-load hook for Claude Code (SessionStart, PreCompact).

Primes the agent with MEMORY.md (L1) plus the scope-first memory
principle. Same content the OpenCode memory-load plugin injects via
chat.message.
"""
import json
import os
import sys
from pathlib import Path


SCOPE_FIRST_PRINCIPLE = """--- Memory Scope-First Principle ---
Before writing anything to memory, decide the scope:
- Project-wide fact / preference / goal -> MEMORY.md (L1)
- Per-corpus knowledge -> memory/understanding/<workspace>.md (L2)
- Per-corpus personal state (TODOs, shortcuts, ad-hoc notes)
    -> memory/<kind>/<workspace>.md  (create on demand)
- Historical / searchable -> memory/journal/<workspace>/<date>-<title>.md (L3)

Prefer the narrower scope. Never dump per-corpus content into MEMORY.md
or memory/journal/general/. See /remember for the full guide.
--- End Principle ---"""


def main() -> int:
    cwd = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())
    parts: list[str] = []
    memory_md = cwd / "MEMORY.md"
    if memory_md.is_file():
        try:
            body = memory_md.read_text(encoding="utf-8")
        except OSError:
            body = ""
        if body:
            parts.append("--- Project Memory (MEMORY.md) ---")
            parts.append(body.rstrip())
            parts.append("--- End Project Memory ---")
            parts.append("")
    parts.append(SCOPE_FIRST_PRINCIPLE)
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
