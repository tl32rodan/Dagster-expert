<!-- all-might generated -->
# Refusal patterns — cross-mode mandatory templates

This file standardizes the refusal language across CHARTER, CODE, and
SMOKE modes. The auditor never invents free-form rejection prose;
it picks the matching template here and fills the slots.

## The one canonical template

```
REJECT: <rule-id>:
  <location>: <one-line gap statement>
  Remediation: <exact command, file, or rewrite to apply>
  Source: <path to the rule definition>
```

Slot rules:

- `<rule-id>` is a dotted identifier. See "Rule ID registry" below.
- `<location>` is either:
  - `<path>:<line>` (CODE mode line-level finding), or
  - `<audit-file>::<criterion-id>` (CHARTER mode), or
  - `<smoke-row-id>` (SMOKE mode).
- `<one-line gap statement>` describes the *observed* gap in past
  tense — what was found, not what we wish.
- `<remediation>` is **executable**: a shell command, an Edit
  instruction, a file path to write, or a precise prose rewrite. No
  "consider…", no "you might want to…". Use imperative.
- `<source>` cites the rule's home file, including the section /
  rule number where applicable.

## Why one template

Less-capable agents reproduce the template verbatim; reviewers can
scan a column of REJECTs and find the offender at a glance. The
template is the contract.

## Rule ID registry

### CHARTER-mode rule IDs (from audits/0N-….md)

| Rule ID | Source |
|---|---|
| `01.C1` … `01.C6` | `audits/01-state-management.md` |
| `02.C1` … `02.C6` | `audits/02-stop-and-rerun.md` |
| `03.C1` … `03.C7` | `audits/03-job-scheduling.md` |
| `04.C1` … `04.C6` | `audits/04-dependency-definition.md` |
| `05.C1` … `05.C7` | `audits/05-logs-and-env-status.md` |

### CODE-mode rule IDs (from standards/)

| Rule ID | Source |
|---|---|
| `tdd-rule.test-first` | `standards/tdd-rules.md` |
| `tdd-rule.test-after-impl` | `standards/tdd-rules.md` |
| `tdd-rule.test-trivial` | `standards/tdd-rules.md` |
| `tdd-rule.test-misplaced` | `standards/tdd-rules.md` |
| `tdd-rule.refactor-without-test-evidence` | `standards/tdd-rules.md` |
| `tdd-rule.mock-fabricates-api` | `standards/tdd-rules.md` |
| `clean-code.naming` | `standards/clean-code-rules.md` |
| `clean-code.srp` | `standards/clean-code-rules.md` |
| `clean-code.dead-code` | `standards/clean-code-rules.md` |
| `clean-code.no-shim` | `standards/clean-code-rules.md` |
| `clean-code.comment-explains-what` | `standards/clean-code-rules.md` |
| `clean-code.complexity` | `standards/clean-code-rules.md` |
| `clean-code.premature-abstraction` | `standards/clean-code-rules.md` |

### SMOKE-mode rule IDs (from smoke/)

| Rule ID | Source |
|---|---|
| `smoke.state-checkpoint.dagster-home` | `smoke/README.md` |
| `smoke.state-checkpoint.ap-src` | `smoke/README.md` |
| `smoke.state-checkpoint.venv` | `smoke/README.md` |
| `smoke.state-checkpoint.dagster-version` | `smoke/README.md` |
| `smoke.assert.exit-code` | `smoke/cli-conformance.md` |
| `smoke.assert.stdout-shape` | `smoke/cli-conformance.md` |
| `smoke.assert.wall-time` | `smoke/cli-conformance.md` |
| `smoke.assert.graphql-response` | `smoke/graphql-conformance.md` |
| `smoke.assert.no-row` | `smoke/README.md` |

### Shared rule IDs (apply to any mode)

| Rule ID | Meaning |
|---|---|
| `shared.private-import` | Plan / diff / smoke script references `dagster._core.*` / `_internal.*` / `_private.*` |
| `shared.uv-or-dg` | Plan / diff / smoke script uses `uv`, `dg`, `pipx`, Poetry, k8s, Helm, public PyPI, or Docker registries |
| `shared.public-pypi` | Plan / diff / smoke script reaches public PyPI at runtime |
| `shared.no-citation` | Claim made without `$AP_SRC/<file>:<line>` or `personalities/...` citation |
| `shared.no-journal` | Verdict emitted but not written to `memory/journal/<workspace>/` |

## Examples (each mode, one example)

### CHARTER example

```
REJECT: 01.C3:
  audits/01-state-management.md::C3: AP partial-completion semantics
  not encoded as a PartitionsDefinition
  Remediation: list the AP sub-units (branches × steps × cells ×
               PVTs × …), compute the leaf cardinality, then pick
               StaticPartitionsDefinition / DynamicPartitionsDefinition
               / MultiPartitionsDefinition per the math
  Source: personalities/dagster-ap-auditor/audits/01-state-management.md §C3
```

### CODE example

```
REJECT: tdd-rule.test-first:
  src/ap_compat/state.py:18: new public function map_status() added
  without matching test in same diff
  Remediation: write tests/ap_compat/test_state.py asserting the
               5 status mappings, commit it FIRST (red), then commit
               the impl that turns it green
  Source: personalities/dagster-ap-auditor/standards/tdd-rules.md §Red-Green-Refactor
```

### SMOKE example

```
REJECT: smoke.state-checkpoint.dagster-home:
  state checkpoint #1: echo $DAGSTER_HOME returned empty
  Remediation: setenv DAGSTER_HOME ~/.dagster-ap-audit       (tcsh)
               export DAGSTER_HOME=~/.dagster-ap-audit       (bash)
               mkdir -p $DAGSTER_HOME && echo $DAGSTER_HOME
  Source: personalities/dagster-ap-auditor/PRE_FLIGHT_CHECKLIST.md Box 2
```

## When you must invent a new rule ID

You probably must not. New rule IDs come from the curator (Brian)
after a case study lands in `memory/lessons_learned/_inbox/`. If you
encounter a gap that doesn't match any registered rule:

1. Use `shared.no-citation` (most catch-all) for the moment.
2. File the case study describing the unrecognized gap.
3. Do NOT invent a rule ID inline; the curator promotes via a new
   commit to this file.
