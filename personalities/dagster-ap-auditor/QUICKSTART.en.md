<!-- all-might generated -->
# dagster-ap-auditor — quickstart (English)

You are talking to a **strict acceptance auditor** for the Dagster
1.13.3 ↔ AP compatibility migration. It has three modes:

| Mode | When to use it |
|---|---|
| **CHARTER** | You have a migration plan or architecture proposal; you want the auditor to verify parity for one or more of the 5 dimensions (state / stop&rerun / scheduling / deps / logs+env). |
| **CODE** | You have a diff (staged, committed, or pasted); you want a TDD + clean-code review with line-level findings. |
| **SMOKE** | You want the auditor to actually run Dagster CLI / GraphQL commands and diff their behavior against the AP contract. |

Pick the mode by saying one of the trigger phrases (see
`MODE_DECISION_TREE.md`) — the auditor will declare the mode out
loud and carry it.

## Before anything: tell the auditor where AP is

The auditor refuses every action until `$AP_SRC` is set:

```
setenv AP_SRC /abs/path/to/ap        # tcsh
export AP_SRC=/abs/path/to/ap        # bash
echo $AP_SRC && ls $AP_SRC | head -5  # verify
```

## I want a parity audit (CHARTER)
> Audit my migration plan for state management.

The auditor enters CHARTER mode, reads
`audits/01-state-management.md`, and demands evidence rows:
- AP behavior cited as `$AP_SRC/<file>:<line>`
- Dagster mapping cited as a path inside
  `personalities/dagster-expert/database/dagster-1.13.3/docs/…`

Verdict is binary: PASS or REJECT. Every REJECT row names the gap and
the remediation.

## I want a code review (CODE)
> Review this diff.

The auditor enters CODE mode and runs:
1. TDD scan — was the test written first?
2. Clean-code 7-point scan — naming / SRP / dead code / no
   backward-compat shims / no WHAT-comments / complexity ≤ 10 / no
   premature abstraction.

Line-level findings come back as `path:line: <rule-id>: <gap>`. No
test → REJECT. Test after impl in commit log → REJECT.

## I want a behavioral smoke test (SMOKE)
> Run the smoke audit for `run terminate`.

The auditor enters SMOKE mode, runs the mapped Dagster CLI / GraphQL
command, and asserts against the row in
`smoke/cli-conformance.md` (or `smoke/graphql-conformance.md`).

Pre-conditions are strict: `$DAGSTER_HOME` non-empty, `$AP_SRC` is a
dir, `dagster --version` reports 1.13.3, `which dagster` is in the
venv. Any miss → REFUSE.

## "I want to share a gotcha"

Tell the auditor `/remember <thing>`. It writes to
`personalities/dagster-ap-auditor/memory/lessons_learned/_inbox/<timestamp>-<user>.md`.
The curator (Brian) audits later.

## Shell note

The user's shell is **tcsh**. The auditor uses `setenv` syntax first;
bash `export` equivalents are shown in parentheses.

## Sibling personality

If you ask the auditor a Dagster *teaching* / *operating* / *API
lookup* question (not a parity audit), it will say "this looks like
dagster-expert's territory, switch?". Say "switch to dagster-expert"
and the daily-driver personality takes over.
