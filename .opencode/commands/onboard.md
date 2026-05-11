<!-- all-might generated -->
Create the project's personalities.

Run once after `allmight init`. The init step is scaffold-only — no
personality exists yet. This command asks you about purpose,
proposes from the suggestion catalog at
`.allmight/suggestions/personalities/`, and creates whichever you
pick by shelling out to `allmight add`.

## What happens

1. Reads `.allmight/onboard.yaml` (written by `allmight init`).
2. Asks ONE question about your purpose.
3. Proposes 1-3 suggestions matched against your purpose, plus the
   `general` fallback.
4. For each chosen suggestion, runs
   `allmight add <name> --capabilities <list>` so the marker,
   capability table, and registry entry are correct by construction.
5. Updates `MEMORY.md` with the `## Project Map` rows and the
   `> **Default personality**: <name>` callout.
6. Marks `onboarded: true` in `.allmight/onboard.yaml`.

## How to execute

Load the `onboard` skill and follow its checklist. The skill body
covers each step; the agent should not free-form ROLE.md content.

## When NOT to run

- The project has no `.allmight/onboard.yaml` — run `allmight init`
  first.
- You're partway through a session that's already onboarded — the
  skill will ask what to redo.
