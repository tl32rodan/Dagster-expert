<!-- all-might generated -->
# Finding & refusal patterns — verify-tick templates

This file standardizes how the **verify** tick writes a finding when an
increment fails a check, and how any tick writes a refusal when a hard
rule is violated. Don't invent free-form prose; pick the template and
fill the slots. (Evolved from the retired auditor's cross-mode refusal
file; the CHARTER/CODE/SMOKE framing is gone.)

## Verify finding format (written to `flow-model/_open_questions.yaml::findings`)

```yaml
- increment: <id>
  date: YYYY-MM-DD
  type: <fail-code>          # see registry below
  detail: "<one line: what was observed, where — past tense>"
  fix_hint: "<one line, imperative: what the next build must do>"
```

The increment goes `blocked`; the next `build` tick re-picks it and
fixes it before advancing. No "done with caveats".

## Refusal format (any tick, when a hard rule blocks work)

```
REFUSE: <rule-id>:
  <location>: <one-line gap, past tense>
  Remediation: <exact command / file / rewrite — imperative, executable>
  Source: <path to the rule's home>
```

`<location>` is `<path>:<line>` for code, `<step/increment id>` for the
flow, or a checklist row `conversion-coverage/0N-*.md::C<id>`.

## Verify FAIL-code registry (the six checks)

| `type` | Meaning | Source of the rule |
|---|---|---|
| `invented-api` | a `from dagster import` symbol is not in the 1.13.3 corpus | `dagster-expert/database/dagster-1.13.3/docs/` |
| `private-import` | references `dagster._core/_internal/_private` | `ROLE.md §2`, `shared.private-import` |
| `smoke-failed` | the increment's `accept` command did not exit 0 | `smoke/` |
| `not-converted` | a source file was copied in, not expressed as asset/partition/config | `ROLE.md §2`, `build-loop §3` |
| `uncited` | the build journal names no `$FLOW_SRC/<file>:<line>` + API | `ROLE.md §3.7` |
| `coverage-gap` | a `conversion-coverage/0N-*.md` aspect is neither preserved nor parked | `conversion-coverage/` |

## Shared rule IDs (any tick)

| Rule ID | Meaning |
|---|---|
| `shared.private-import` | code references `dagster._core.*` / `_internal.*` / `_private.*` |
| `shared.air-gap` | uses `uv` / `dg` / `pipx` / Poetry / k8s / public PyPI / Docker |
| `shared.no-citation` | a claim without a `$FLOW_SRC/<file>:<line>` or `personalities/...` cite |
| `shared.no-handoff` | a tick ran but did not update `STATUS.md` + ledger + ops-log |
| `shared.guessed` | invented flow behavior / API instead of parking an open question |

## Optional deeper code checks (verify MAY apply, not required)

The `standards/tdd-rules.md` and `standards/clean-code-rules.md` rule
IDs (`tdd-rule.*`, `clean-code.*`) still exist and may be applied to an
increment's code when warranted (e.g. a large factory change). They are
no longer a mandatory gate — the six verify checks above are. Use the
same finding/refusal format with those rule IDs as `<rule-id>`.

## Examples

Finding (verify):
```yaml
- increment: a3
  date: 2026-05-26
  type: not-converted
  detail: "pvt_list.pl copied into pipelines/ unchanged; no config+generator"
  fix_hint: "express as config/pvt.yaml + a generator that writes the per-leaf files"
```

Refusal (pre-flight):
```
REFUSE: shared.no-citation:
  increment a3: claim "lpe fans out per cell" has no $FLOW_SRC cite
  Remediation: grep -n the fan-out in $FLOW_SRC/lpe.pl and cite file:line,
               or park an open question if it can't be determined
  Source: personalities/flow-cartographer/ROLE.md §3.3
```

## When no code fits

Don't invent a new code inline. Use the closest (`shared.guessed` or
`coverage-gap`), park the specifics in `_open_questions.yaml`, and let
the `reflect` tick propose a new standard to the curator. New codes
come from a curator commit, never mid-tick.
