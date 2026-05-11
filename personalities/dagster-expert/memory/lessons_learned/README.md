<!-- all-might generated -->
# lessons_learned/ — curator-audited memory

Two subdirectories, two roles:

## `_inbox/` — write freely

When you (agent or human) hit an operational gotcha, a learner stuck
point, a librarian doc gap, or any reusable observation, write a
case-study markdown file here. Filename:

```
<ISO-timestamp>-<unix-user>.md
```

e.g. `2026-05-11T14-22-08-brian.md`.

Per-user + per-timestamp prevents collisions when multiple users / agents
write concurrently (shared NFS-hosted instance mode).

Suggested case-study template:

```markdown
# Title — one line

## Symptom
What happened. Concrete: command run, error message, observed behavior.

## Context
Dagster version, DAGSTER_HOME, venv path, which lesson or which deploy.

## Root cause
What was actually wrong. If unknown, say so.

## Fix
Exact commands or config change. Copy-paste-runnable.

## Generalizes to
Pattern: "anyone using <X> hits this when <Y>". Empty if one-off.
```

## `_reviewed/` — curator-only

The curator (Brian) audits `_inbox/` periodically and either:

- **Promotes** the case study into a new cheatsheet entry under
  `personalities/dagster-expert/database/dagster-1.13.3/docs/<topic>.md`
- **Folds** the case study into an existing entry (e.g. add a "Gotcha"
  subsection)
- **Moves** the case study verbatim into `_reviewed/` for archival
- **Discards** if environment-specific or wrong

After any promotion, re-`/ingest` the `dagster-1.13.3` workspace so SMAK
indices reflect the new content.

**Agents: never write to `_reviewed/`.** That's curator-only. Always
use `_inbox/`.
