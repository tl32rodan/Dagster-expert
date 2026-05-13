<!-- all-might generated -->
# TDD rules — CODE-mode mandatory standard

This file defines the **mechanical** TDD requirements for any diff
under `dagster-ap-auditor`'s CODE-mode review. The auditor reads it
once per CODE-mode invocation and uses the checks below to produce
line-level findings.

## Red → Green → Refactor

Every implementation change must follow this order:

1. **Red.** Write a test that exercises the new behavior. Run the test
   suite — the test FAILS (the impl doesn't exist yet, or the
   behavior is wrong).
2. **Green.** Write the smallest impl that makes the test PASS. Do
   not over-build.
3. **Refactor.** Clean the impl with the test as a safety net. The
   test still PASSES after the refactor.

**Evidence we accept in CODE-mode:**

- The test file and the impl file appear in the same diff, AND
- The commit log shows the test file committed BEFORE or WITH the
  impl file (use `git log --oneline -- <test-file>` and
  `git log --oneline -- <impl-file>` and compare).

If the test is committed **after** the impl, the diff fails CODE-mode.

## Test file placement (mechanical)

For an implementation file at:
```
src/<package>/<module>.py
```

The test file MUST be at one of:
```
tests/<package>/test_<module>.py
tests/test_<module>.py
src/<package>/tests/test_<module>.py
```

If the test file is elsewhere, the diff must include a short note in
the commit body explaining the layout and pointing at the project
test convention.

## Test contents — minimum bar

A new test file must contain at least one test that:

1. **Imports the public symbol(s)** the impl exports.
2. **Sets up an input** representative of the AP behavior under audit.
3. **Asserts an output / state / side-effect** that distinguishes
   correct from incorrect impl.
4. **Would FAIL** if the impl line(s) under review were absent.

A "smoke test" that only checks `import works` is **insufficient**.

## Refactor-only diffs

A diff that touches only existing code (no new public behavior) is
acceptable without a new test **if and only if**:

1. The existing tests still PASS without modification.
2. The commit message says "refactor" (no functional change).
3. The diff does not introduce a new branch / new public symbol.

The auditor verifies (1) by running the test suite if it's available;
otherwise it REJECTS with `tdd-rule.refactor-without-test-evidence`.

## Test-double rules

For unit tests that mock collaborators:

- Mocks must use **real names** of the collaborator (e.g.
  `MagicMock(spec=DagsterRun)`), not opaque stubs.
- A mock of a Dagster public symbol must be backed by a real
  reference to that symbol in
  `personalities/dagster-expert/database/dagster-1.13.3/docs/…`.
  Otherwise the mock is fabricating an API.

## Mapping rule IDs

Refusal language refers to these IDs:

| Rule ID | Meaning |
|---|---|
| `tdd-rule.test-first` | Impl committed without a matching test in the same diff |
| `tdd-rule.test-after-impl` | Test commit timestamp later than impl commit timestamp |
| `tdd-rule.test-trivial` | New test exists but doesn't assert anything that would FAIL without the impl |
| `tdd-rule.test-misplaced` | Test file not in any of the accepted paths and no convention note in commit body |
| `tdd-rule.refactor-without-test-evidence` | Refactor-only diff but test suite not run / no evidence of green state |
| `tdd-rule.mock-fabricates-api` | A mock references a Dagster symbol that does not appear in the dagster-expert corpus |

## Output format

When CODE-mode emits a TDD finding, use this exact shape (per
`standards/refusal-patterns.md`):

```
REJECT: <rule-id>:
  <path>:<line>: <one-line gap>
  Remediation: <exact command or file to fix>
  Source: personalities/dagster-ap-auditor/standards/tdd-rules.md §<section>
```

The auditor groups all TDD findings into a single block, then
proceeds to the clean-code scan.
