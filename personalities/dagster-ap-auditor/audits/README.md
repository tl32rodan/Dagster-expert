<!-- all-might generated -->
# audits/ — Phase-1 Dagster ↔ AP parity checklists

This directory holds five mechanical checklists, one per dimension.
The CHARTER mode of `dagster-ap-auditor` reads exactly one (or more)
of these files per audit and produces a binary PASS / REJECT verdict.

## How to use a checklist

1. Identify the dimension from the user's request. The mapping is:

   | # | Dimension | File |
   |---|---|---|
   | 01 | State management | `01-state-management.md` |
   | 02 | Stop & rerun | `02-stop-and-rerun.md` |
   | 03 | Job scheduling | `03-job-scheduling.md` |
   | 04 | Dependency definition | `04-dependency-definition.md` |
   | 05 | Logs & env status | `05-logs-and-env-status.md` |

2. `Read personalities/dagster-ap-auditor/audits/0N-…md` end to end.

3. For each "Parity criterion (C1, C2, …)", gather **two** pieces of
   evidence:
   - **AP-side**: `$AP_SRC/<file>:<line>` showing the behavior.
   - **Dagster-side**: a public-API symbol from
     `personalities/dagster-expert/database/dagster-1.13.3/docs/…` or
     a lesson from `personalities/dagster-expert/learn/<NN>-…`.

4. Fill the Evidence table at the bottom of the checklist. Each row
   is PASS or FAIL. Overall verdict = PASS iff every row is PASS.

5. Write the verdict to
   `personalities/dagster-ap-auditor/memory/journal/<workspace>/<YYYY-MM-DD>-charter-<dim>-<short-title>.md`
   with citations inline.

## Uniform template (every 0N-….md file follows this)

```markdown
# Audit: <Dimension Name>

## AP behavior (must cite from $AP_SRC)
Required reading paths (use `grep -rn "<keyword>" $AP_SRC`):
- <subdir / module hint 1>
- <subdir / module hint 2>

Expected behaviors (AP-side, to be confirmed by `$AP_SRC` reading):
- B1: <one-line behavior>
- B2: ...

## Dagster 1.13.3 corresponding API
Source: personalities/dagster-expert/database/dagster-1.13.3/docs/<file>.md
Also see: personalities/dagster-expert/learn/<NN>-…/README.md
- API 1: <name + reference>
- API 2: ...

## Parity criteria (PASS only if ALL true)
- [ ] C1: <criterion>
- [ ] C2: ...
- [ ] C3: ...

## Refusal triggers (mechanical)
- C1 unmet → "REJECT: <dim>.C1: <gap>; Remediation: <cmd>"
- C2 unmet → ...

## Evidence template
| Criterion | AP source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $AP_SRC/... | docs/...md::... | PASS / FAIL |
| C2 | ... | ... | PASS / FAIL |
```

## Hard rules across all dimensions

1. **No verbal-only claims.** Every PASS row needs both columns
   filled with real paths.
2. **No private Dagster imports** (`dagster._core.*` / `_internal.*` /
   `_private.*`) in any proposed mapping. If the public API is
   missing, the row is FAIL — not "use private".
3. **No "should work" language.** Restate as "<AP behavior X> maps
   to <Dagster API Y> because <evidence Z>".
4. **Binary verdict.** No "partial PASS" / "PASS with caveats".
5. **Curator-only additions.** New rows in `Parity criteria` come from
   the curator (Brian) after a case study lands in
   `memory/lessons_learned/_inbox/`. The agent does NOT auto-add rows
   during an audit.

## When a dimension turns up a gap that has no PASS path

That's exactly what this catalog is for. Write:

```
REJECT: <dim>.<criterion-id>: <gap>
Remediation: file a case study at
  personalities/dagster-ap-auditor/memory/lessons_learned/_inbox/<ISO>-<unix_user>.md
  describing the AP behavior and the missing Dagster public API.
Source: personalities/dagster-ap-auditor/audits/<file>.md row C<id>
```

The migration plan does not pass until either (a) the gap is closed
with a public-API mapping, or (b) the criterion is formally retracted
by the curator (which happens via a new commit to this checklist,
never inline during an audit).
