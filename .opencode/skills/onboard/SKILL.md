---
name: onboard
description: Finish All-Might setup: capture user intent in each personality's ROLE.md and classify the folders listed during init.
---

<!-- all-might generated -->

# Onboard — create personalities

Run after `allmight init` to create the project's personalities.
State: `.allmight/onboard.yaml` has `onboarded: false, personalities: []`.

## Steps

### 1. Read state
```bash
cat .allmight/onboard.yaml
```
If `onboarded: true`, ask the user what to change before re-running.

### 2. Ask once
> "Do you have a specific purpose for this project, or should I set up
> a general-purpose assistant?"

### 3. Match suggestions
Read the suggestion catalog:
```bash
ls .allmight/suggestions/personalities/
```
Each YAML has `name`, `capabilities`, `scope`, `keywords`. If the
user described a purpose, score each suggestion's `keywords` against
their words and present the top 1-3 candidates. Always include
`general` as the fallback. Let the user pick one or more.

### 4. Create personalities (mechanical — DO NOT free-form)
For each chosen suggestion `<name>`:
```bash
allmight add <name> --capabilities <comma-separated-list-from-yaml>
```
This writes:
- `personalities/<name>/` directory with capability subdirs
- `personalities/<name>/ROLE.md` with the marker + capability table
  (correct by construction — do not edit the marker or table)
- A registry row in `.allmight/personalities.yaml`

### 5. (Optional) Refine ROLE.md scope
If the user gave specific scope words, edit only the `## Scope`
section of `personalities/<name>/ROLE.md` to reflect them. Leave the
marker (`<!-- all-might generated -->`) and capability table alone.

### 6. Update MEMORY.md
- Append one row per created personality to the `## Project Map` table.
- Write `> **Default personality**: <name>` at the very top of
  `MEMORY.md` (above any existing content):
  - 1 personality created → use that name
  - N personalities → ask the user which is the default

The exact format `> **Default personality**: <name>` is parsed by
command bodies — keep it verbatim (blockquote, bold label, single
space, name).

### 7. Mark complete
Edit `.allmight/onboard.yaml` and set `onboarded: true`. Don't touch
the rest of the file.

### 8. Closing message
> Created: <name1>, <name2>. Default: <name>.
> Try `/search "<query>"` or just start asking questions.

## Important
- The capability table inside each ROLE.md is written by
  `allmight add` — never edit it manually. To change a personality's
  command set, edit the suggestion YAML and re-run.
- Suggestions are at `.allmight/suggestions/personalities/`, NOT
  `.allmight/templates/` (which is the `/sync` re-init staging area).
- If `.allmight/onboard.yaml` is missing entirely, the project
  wasn't initialised — tell the user to run `allmight init` first.
