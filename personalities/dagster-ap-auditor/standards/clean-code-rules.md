<!-- all-might generated -->
# Clean-code rules — CODE-mode mandatory standard

Seven mechanical checks. CODE-mode runs all seven on every changed
line in the diff and produces a finding per violation. The auditor
does not "weigh tradeoffs" or "balance concerns" — if a rule is
violated, it's REJECT.

These rules align with the user prefs in `MEMORY.md` ("Don't add
features beyond what the task requires", "Default to writing no
comments", etc.) and with the project-wide "less-capable agent"
design philosophy.

---

## Rule 1 — Descriptive naming

Identifiers describe their role in plain English. No abbreviations
that aren't already in the project's glossary.

- **REJECT** if you see `tmp`, `data`, `obj`, `val`, `res`, `ret`,
  `arr`, `dict_`, `lst`, `cfg`, `mgr`, `srv`, `proc` used as
  variable / function / class names without a domain qualifier
  (`tmp_event_log`, `result_partition`).
- **REJECT** if you see single-letter names outside of loop counters
  (`i`, `j`) or coordinate triples (`x`, `y`, `z`).
- **REJECT** if a function name doesn't include a verb (`validate_run`
  good; `run_validation` good; `run_thing` bad).

Rule ID: `clean-code.naming`

## Rule 2 — Single responsibility per function

A function does one thing. The auditor counts "things" by
side-effect class:

- Reads input → 1
- Validates input → 1
- Mutates state → 1
- Writes output → 1
- Calls external service → 1

If a function does more than **2** of these things, REJECT.

Rule ID: `clean-code.srp`

## Rule 3 — No dead code

The diff must not introduce:

- Unused imports
- Unused local variables (including `_unused` placeholders for
  intentional ignores — Python idiom is `_`, not `_unused_var_name`,
  except when destructuring tuples)
- Unreachable branches (code after an unconditional `return`,
  `raise`, `continue`)
- Functions / classes never called from the rest of the diff or from
  the existing codebase (verified by `grep -rn`)
- `# TODO` / `# FIXME` / `# XXX` comments without an associated
  tracking issue link in the commit body

Rule ID: `clean-code.dead-code`

## Rule 4 — No backward-compat shims for nonexistent old code

If a diff introduces a "deprecated" alias, a wrapper that "preserves
the old name", or a fallback branch handling "the old format" —
**REJECT** unless the commit body cites the AP-side caller that
still needs the old shape.

Backward-compat is only justified by a real consumer. Imaginary
consumers don't count. The MEMORY.md user pref is explicit: "Avoid
backwards-compatibility hacks like renaming unused _vars, re-exporting
types, adding // removed comments for removed code".

Rule ID: `clean-code.no-shim`

## Rule 5 — No "explains WHAT" comments

A comment is justified only if it explains a non-obvious **WHY**: a
hidden constraint, a subtle invariant, a workaround for a specific
bug, behavior that would surprise a reader. Comments that paraphrase
the code below them — **REJECT**.

Examples of WHAT-comments (REJECT):
```python
# Increment counter
counter += 1

# Loop over the items
for item in items:
    ...

# Return the result
return result
```

Examples of WHY-comments (ACCEPT, if true):
```python
# AP's scheduler emits ticks at second boundaries, so subtracting
# 1ms here prevents off-by-one on the very first tick of a minute.
deadline = next_tick - timedelta(milliseconds=1)
```

Rule ID: `clean-code.comment-explains-what`

## Rule 6 — Cyclomatic complexity ≤ 10

For each function in the diff, count branches: each `if`, `elif`,
`for`, `while`, `case`, `try`, `except`, `and`, `or` adds 1 to the
complexity, starting from 1.

- ≤ 10: OK.
- 11–14: REJECT, must refactor (extract helper, dispatch table, etc.).
- ≥ 15: REJECT, treat as a structural problem.

Rule ID: `clean-code.complexity`

## Rule 7 — No premature abstraction

The diff must not introduce:

- A new base class / protocol / interface with **only one** concrete
  implementation in the same diff.
- A new factory / registry / dispatch table for **fewer than three**
  use cases.
- A new "configuration" parameter that has only one allowed value
  today.
- A new helper that wraps a single standard-library or framework call
  with no additional logic.

MEMORY.md user pref: "Three similar lines is better than a premature
abstraction. No half-finished implementations either."

Rule ID: `clean-code.premature-abstraction`

---

## Output format

When CODE-mode emits a clean-code finding, use the standard refusal
template (see `standards/refusal-patterns.md`):

```
REJECT: <rule-id>:
  <path>:<line>: <one-line gap>
  Remediation: <exact command or file to fix>
  Source: personalities/dagster-ap-auditor/standards/clean-code-rules.md §<rule>
```

## Mapping all rule IDs

| Rule ID | Rule |
|---|---|
| `clean-code.naming` | Rule 1 — Descriptive naming |
| `clean-code.srp` | Rule 2 — Single responsibility |
| `clean-code.dead-code` | Rule 3 — No dead code |
| `clean-code.no-shim` | Rule 4 — No backward-compat shims |
| `clean-code.comment-explains-what` | Rule 5 — No "explains WHAT" comments |
| `clean-code.complexity` | Rule 6 — Cyclomatic complexity ≤ 10 |
| `clean-code.premature-abstraction` | Rule 7 — No premature abstraction |
