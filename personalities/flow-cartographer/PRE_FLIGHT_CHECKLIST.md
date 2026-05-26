<!-- all-might generated -->
# flow-cartographer — session pre-flight checklist

**Run this at Step 0 of the Wake SOP, on every session and every
scheduled tick, BEFORE doing real work.** Tick every box out loud ("Box
N: yes/no/n/a"). If a gating box fails, STOP and resolve it first.

This file exists because less-capable agents skip implicit
pre-conditions. Make them explicit and the agent won't skip them.

---

## Box 1 — Shell awareness
The user is on **tcsh**. tcsh-first in every example:

| Want | tcsh (default) | bash (also show) |
|---|---|---|
| Set env | `setenv NAME value` | `export NAME=value` |
| Unset | `unsetenv NAME` | `unset NAME` |
| Activate venv | `source ~/dagster-venv/bin/activate.csh` | `source ~/dagster-venv/bin/activate` |

## Box 2 — `$FLOW_SRC` (required, ALL ticks)
`$FLOW_SRC` MUST point at an existing directory: the source flow to
convert. Verify:
```
echo $FLOW_SRC && ls $FLOW_SRC | head -5    # expect a dir listing
```
Empty or not-a-dir → **REFUSE**:
- `setenv FLOW_SRC /abs/path/to/flow` (tcsh) or
  `export FLOW_SRC=/abs/path/to/flow` (bash), then re-verify.

## Box 3 — `CONVERSION.md` charter (required, ALL ticks)
`personalities/flow-cartographer/CONVERSION.md` MUST exist and have no
`[PLACEHOLDER]` left in **Flow identity / Goal / Success criteria**.
```
grep -n "PLACEHOLDER" personalities/flow-cartographer/CONVERSION.md
```
Placeholders remain → this is a **setup turn**: help the user fill the
charter, then stop. Do NOT plan/build against an unfilled charter.

## Box 4 — `$DAGSTER_HOME` (required for build + verify)
`echo $DAGSTER_HOME` MUST be a non-empty absolute path before any
`dagster*` command or smoke run.
- `setenv DAGSTER_HOME ~/.dagster-cartographer` (tcsh) /
  `export DAGSTER_HOME=~/.dagster-cartographer` (bash); `mkdir -p $DAGSTER_HOME`.
- Kept separate from teacher (`~/.dagster-tutor/<NN>`) and operator
  (`/var/lib/dagster`) homes so conversion runs don't pollute them.

In `plan` / `reflect` ticks (no live Dagster invocation) this is
**n/a** — record "Box 4: n/a".

## Box 5 — venv + version (required for build + verify)
```
which dagster        # must be inside the venv, not /usr/bin
dagster --version    # must report 1.13.3
```
Not in venv → `source ~/dagster-venv/bin/activate.csh`. Not 1.13.3 →
**REFUSE** (the whole target is pinned to 1.13.3). n/a for plan/reflect.

## Box 6 — Read the handoff (the most important box)
You MUST read these before acting, and state in one line where the last
tick left off:
```
personalities/flow-cartographer/STATUS.md            # next_action, active_increment
personalities/flow-cartographer/flow-model/_plan.yaml # the ledger (authoritative)
```
If you have not read the handoff, you are not cleared to proceed. This
is the box that prevents "forgot the earlier requirements / started over".

## Box 7 — Sibling-read armed (read-only)
The `role-load` hook injects all `personalities/*/ROLE.md`, so
`dagster-expert`'s ROLE is in context. Internalize:
- I MAY Read `personalities/dagster-expert/database/dagster-1.13.3/docs/*`,
  `…/learn/<NN>-…/`, `…/demo/scale-lib/` as ground truth.
- I MUST run `dagster-expert/skills/lookup-api/SKILL.md` before writing
  any `from dagster import …`.
- I MUST NOT duplicate that material here, and MUST NOT write into
  `personalities/dagster-expert/…`.

## Box 8 — Park-don't-guess + convert-don't-copy posture armed
Internalize, replacing the old 永不妥協 gatekeeper posture:
- Missing flow info → park an entry in `flow-model/_open_questions.yaml`,
  mark the increment `blocked`. Never invent behavior or API.
- A source file copied in unchanged = a `not-converted` failure. Always
  express it as a Dagster asset / partition / config-driven generator.
- Every tick writes the handoff (Wake SOP Step 4) and a journal entry.
  No handoff update = the tick didn't happen.

---

If all gating boxes pass (Box 4/5 n/a in plan/reflect), state
**"pre-flight complete"**, then continue the Wake SOP (read handoff →
route → run → write handoff).
