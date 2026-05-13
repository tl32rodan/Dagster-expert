<!-- all-might generated -->
# smoke/ — SMOKE-mode conformance reference

The SMOKE mode of `dagster-ap-auditor` **executes** Dagster CLI
commands and Dagster GraphQL queries on the air-gap workstation,
captures their behavior, and diffs it against the AP contract. This
directory holds the per-command and per-query contract rows.

## Files

| File | Purpose |
|---|---|
| `cli-conformance.md` | AP CLI verb ↔ Dagster CLI command mapping; per row: assertions on exit code, stdout shape, wall time |
| `graphql-conformance.md` | AP query verb ↔ Dagster GraphQL query mapping; per row: assertions on response JSON shape |

## State checkpoint (FOUR lines, every SMOKE invocation)

Before any command is dispatched, print and verify:

```
echo $DAGSTER_HOME        # must be a non-empty absolute path
echo $AP_SRC              # must be a non-empty existing dir
which dagster             # must point inside the activated venv
dagster --version         # must report 1.13.3
```

Any failure produces an immediate REJECT with the matching rule ID
from `standards/refusal-patterns.md`:

- `smoke.state-checkpoint.dagster-home`
- `smoke.state-checkpoint.ap-src`
- `smoke.state-checkpoint.venv`
- `smoke.state-checkpoint.dagster-version`

## Per-row assertion contract

Every row in `cli-conformance.md` and `graphql-conformance.md`
declares:

1. **AP verb** — what the AP CLI / API surface calls the operation.
2. **Dagster command / GraphQL query** — the air-gap-safe Dagster
   invocation that should produce equivalent behavior. Absolute paths
   only; never `cd` chains; always include `-w
   /abs/path/workspace.yaml` where Dagster requires a workspace.
3. **Expected exit code** (CLI only).
4. **Expected stdout shape** — a small JSON / text excerpt OR a regex
   the auditor will match against the actual stdout.
5. **Expected wall-time bound** (CLI only; optional).
6. **Expected GraphQL response** (GraphQL only) — a JSON fixture or
   shape descriptor.
7. **Refusal rule ID** if any assertion fails.

## Running a row

For CLI:

```
# Setup
setenv DAGSTER_HOME ~/.dagster-ap-audit            # tcsh
# (or export DAGSTER_HOME=~/.dagster-ap-audit      # bash)
mkdir -p $DAGSTER_HOME
echo $DAGSTER_HOME && echo $AP_SRC && which dagster && dagster --version

# Dispatch the row
<dagster command, absolute path, no cd>

# Verify
echo "exit=$?"
# Compare stdout against the row's shape regex / excerpt
```

For GraphQL:

```
# Confirm webserver is up
nc -zv localhost 3000

# Dispatch the row
dagster-graphql --workspace /abs/path/to/workspace.yaml --query '<query>' --variables '<vars>'

# Capture, diff against the fixture
```

The auditor captures stdout / stderr / exit code / wall time and
compares against the row. Anything off → REJECT with the row's rule
ID.

## No partial pass

If you ran five rows and four passed, the verdict for the overall
delivery is REJECT. Each PASS row goes in the evidence table; the
REJECT row gets a full refusal block; the overall verdict line says
REJECT with the count.

## Where smoke verdicts are journaled

Same as CHARTER and CODE:

```
personalities/dagster-ap-auditor/memory/journal/<workspace>/<YYYY-MM-DD>-smoke-<short-title>.md
```

Include the exact command, the exact output, the assertion that
matched / didn't match, and the row reference.
