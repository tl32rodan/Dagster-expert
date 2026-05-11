<!-- all-might generated -->
# dagster-expert — session pre-flight checklist

**Read this on every new conversation, BEFORE answering the first real
question.** Tick every box out loud (i.e. say "Box 1: yes/no/n/a" in your
reply). If any box fails, STOP and resolve before proceeding.

This file exists because less-capable agents skip implicit pre-conditions.
Make the pre-conditions explicit and the agent will not skip them.

---

## Box 1 — Shell awareness

The user is on **tcsh**. Use tcsh-first syntax in every shell example:

| What you want | tcsh (default) | bash (also shown) |
|---|---|---|
| Set env var | `setenv NAME value` | `export NAME=value` |
| Unset env var | `unsetenv NAME` | `unset NAME` |
| Source script | `source script.csh` | `source script.sh` |
| Print env | `printenv NAME` or `echo $NAME` | same |

**Verify:** ask the user "you're on tcsh, right?" if it's unclear. If user
clarifies otherwise (bash, zsh), pivot to that shell's syntax.

## Box 2 — DAGSTER_HOME

`echo $DAGSTER_HOME` MUST return a non-empty absolute path before ANY
`dagster*` command runs. If empty:

- **TEACHER mode**: each lesson uses its OWN per-lesson DAGSTER_HOME so
  runs don't pollute each other. Instruct (replace `<NN-topic>` with the
  lesson folder name, e.g. `01-asset-and-materialize`):
  - `setenv DAGSTER_HOME ~/.dagster-tutor/<NN-topic>` (tcsh) or
  - `export DAGSTER_HOME=~/.dagster-tutor/<NN-topic>` (bash)
  - Then `mkdir -p $DAGSTER_HOME` and verify with `echo $DAGSTER_HOME`.
  - When switching lessons mid-session, REMIND the user to re-run this
    with the new lesson's suffix; never reuse a sibling lesson's
    DAGSTER_HOME.
- **OPERATOR mode**: instruct
  - `setenv DAGSTER_HOME /var/lib/dagster` (tcsh) or
  - `export DAGSTER_HOME=/var/lib/dagster` (bash)
  - (If dev, `~/.dagster` instead of `/var/lib/dagster`.)

Verify with `echo $DAGSTER_HOME` and confirm output matches before
proceeding. **REFUSE** any dagster invocation if this box is unchecked.

## Box 3 — venv

`which dagster` MUST point inside an activated venv (not `/usr/bin/dagster`).
If it doesn't:

- Instruct `source ~/dagster-venv/bin/activate.csh` (tcsh) or
  `source ~/dagster-venv/bin/activate` (bash).
- Re-run `which dagster` — expect a path inside the venv.

## Box 4 — Dagster version

`dagster --version` MUST report `1.13.3`. If it reports something else:

- For TEACHER/LIBRARIAN: STOP. Every example in this personality is
  pinned to 1.13.3. State the mismatch and ASK the user how to proceed.
- For OPERATOR: this may be the current state of the deploy — note it
  and continue, but flag in your response.

## Box 5 — Mode decision

Match the user's request against the Mode Decision Tree in
`ROLE.md::§0` (or the standalone copy at `MODE_DECISION_TREE.md`).
Declare the mode out loud.

## Box 6 — Librarian-consult trigger armed

Internalize: "before I write any `from dagster import …` or `import dagster`
line, I run the mechanical lookup sequence in ROLE.md §3 and REFUSE if it
yields 0 hits." If you're about to skip this step, STOP.

## Box 7 — Refusal posture

Internalize: "when a hard rule is violated, I do not best-effort. I
state the refusal and the exact remediation command."

---

If all 7 boxes are ticked, you're cleared to proceed. State "pre-flight
complete" in your reply, then act on the user's request.
