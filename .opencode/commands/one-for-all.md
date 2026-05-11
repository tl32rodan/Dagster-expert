<!-- all-might generated -->
Bundle a personality for transfer to another All-Might project.

Run this command when the user asks to export a personality. The
agent applies per-capability rules, reviews content for PII, asks
for user consent on sensitive files, and writes a directory bundle.

Named after All-Might's quirk: **one personality → one bundle**.
For the inverse (absorb multiple sources into one personality), use
``/all-for-one``.

## What happens

1. Identifies the target personality (explicit user name or
   ``allmight list`` + prompt).
2. Picks the destination dir (default ``./<name>-export/``).
3. For each capability, applies export rules:
   - ``database``: ``config.yaml`` yes; ``store/`` no.
   - ``memory``: ``understanding/`` with review; ``journal/`` only
     with explicit yes; ``store/`` no.
4. Reviews every file for PII; asks user about each hit.
5. Writes ``manifest.yaml`` (version + capabilities + ``derived_from``
   lineage list) plus the approved files.
6. Prints a one-line summary of what was bundled and what was
   skipped.

## How to execute

Load the ``one-for-all`` skill and follow its procedure. The skill
body covers each step (capability rules, PII review, manifest
format, bundle layout) and what to ask the user.

## Receiving end

Two paths on the receiver side, depending on what the receiver wants
to do:

- **Single-bundle install into a fresh name** — run ``allmight
  import <bundle>`` (CLI). Mechanical, no merge, fails if the target
  name already exists.
- **Merge into an existing personality, or combine multiple
  bundles, or fold a bundle in with an existing personality** — run
  ``/all-for-one`` (skill) in the agent. Handles per-file conflicts
  and ROLE.md prose reconciliation.
