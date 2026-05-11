<!-- all-might generated -->
Absorb multiple personalities (bundles or in-project) into a single target.

Run this command when the user asks to merge personalities, fold a
bundle into an existing one, or consolidate in-project roles. Named
after All-Might's antagonist's quirk: **N sources → 1 target**.

For the inverse (export one personality outward as a bundle), use
``/one-for-all``. For bundles arriving over a git remote, ``allmight
share pull`` installs them directly without going through this skill.

## What happens

1. Collect sources from the user — each is either a bundle path
   (``/one-for-all`` output) or an in-project personality name.
2. Identify the target — new name or existing personality.
3. For each capability in the union of sources' capabilities:
   - ``database``: per-workspace; name clashes → user dialog
     (rename / pick / merge corpora).
   - ``memory/understanding/``: per-file; clashes → dialog
     (pick / overwrite / concat / agent-rewrite).
   - ``memory/journal/``: append + sort by timestamp + dedupe.
   - ``ROLE.md``: agent drafts a merged version, user confirms.
   - ``store/`` (any): never copied; rebuild via ``/ingest``.
4. Update ``.allmight/personalities.yaml`` with a ``derived_from``
   list recording every source.
5. Ask whether to keep or remove the in-project source personalities
   (default: keep).
6. Print a summary and remind the user to ``/ingest`` merged
   workspaces.

## How to execute

Load the ``all-for-one`` skill and follow its procedure. The skill
body covers source discovery, per-capability merge rules, ROLE.md
drafting, and registry updates.
