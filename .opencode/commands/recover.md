<!-- all-might generated -->
Restore memory data from the recovery snapshot mirror.

Wraps ``allmight memory log/diff/restore`` with the dialog needed to
pick the right snapshot for the typical recovery moment ("I just
deleted that, get it back"). For scripting / power-user flows the
CLI subcommands stay available.

## What happens

1. Identifies the file to recover (from the user's request, or by
   showing recent snapshots and asking).
2. Picks the revision: ``HEAD~1`` for "right before the mistake",
   or a specific sha from ``allmight memory log``.
3. Confirms with the user (shows ``allmight memory diff <sha>`` if
   the choice isn't obvious).
4. Runs ``allmight memory restore <file> --rev <sha> --yes``.
5. Prompts ``/ingest`` if the restored file lives under
   ``personalities/<active>/database/``.

## How to execute

Load the ``recover`` skill and follow its procedure. The skill body
covers the trigger phrases, the dialog steps, and the CLI
invocations.
