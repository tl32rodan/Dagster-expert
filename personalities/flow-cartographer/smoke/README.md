<!-- all-might generated -->
# smoke/ — run-it-for-real inputs for the verify tick

The **verify** tick's "smoke must actually run" check (check 3 in
`skills/verify-loop/SKILL.md`) is what proves a converted increment
works on the air-gap box, not just that it parses.

## Primary smoke: the increment's `accept` command

Every increment in `flow-model/_plan.yaml` carries an `accept:` command
(normally `python -m _smoke`, or
`dagster asset materialize -m <mod> --partition '<k>'` with an absolute
`-w /abs/path/workspace.yaml`). Verify runs it; exit 0 = pass, anything
else = FAIL `smoke-failed` (paste the failing tail into the finding).
State checkpoint first (tcsh-first, absolute paths):

```
echo $DAGSTER_HOME        # non-empty absolute path
echo $FLOW_SRC            # the source flow dir
which dagster             # inside the venv
dagster --version         # 1.13.3
```

Any miss → REFUSE with the matching pre-flight rule (see
`PRE_FLIGHT_CHECKLIST.md` Box 4/5).

## Optional reference: CLI / GraphQL conformance tables

`cli-conformance.md` and `graphql-conformance.md` are **curator-filled
skeletons** carried over from the retired auditor. They map a source-flow
verb (cited `$FLOW_SRC/<file>:<line>`) to an air-gap-safe Dagster CLI
command or GraphQL query, with assertions on exit code / stdout shape /
response JSON. The verify tick MAY consult a row when an increment's
behavior should match a specific CLI/GraphQL contract — but the primary
gate is the increment's own `accept` command above, not these tables.

Rows are added by the curator after a case study lands in
`memory/lessons_learned/_inbox/`; the loop does not auto-add rows
mid-tick. Use absolute paths, never `cd` chains, always
`-w /abs/path/workspace.yaml` where Dagster needs a workspace.
