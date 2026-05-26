<!-- all-might generated -->
# GraphQL conformance — source-flow query ↔ Dagster GraphQL mapping (skeleton)

Each row maps a source-flow API query verb onto a Dagster 1.13.3 GraphQL
query and declares per-assertion expectations on the response JSON.
The verify tick reads one row per smoke run, dispatches the
GraphQL query via `dagster-graphql`, and diffs against the row.

## Endpoint expectations

The Dagster webserver must be running and reachable. The verify tick
checks before dispatching any query:

```
nc -zv localhost 3000        # or whatever the webserver port is
```

If unreachable, REJECT with `smoke.state-checkpoint.dagster-version`
(treated as a state-checkpoint failure — the env isn't ready).

## How to read a row

```
## <row-id>: <human title>

**Source-flow query verb (cite from $FLOW_SRC):**
  $FLOW_SRC/<file>:<line>  →  `<source query name + key inputs>`

**Dagster GraphQL query:**
  ```graphql
  query <name>($input: ...) {
    ...
  }
  ```

**Variables (example):**
  ```json
  { ... }
  ```

**Expected response shape (JSON):**
  ```json
  { ... }
  ```

**Refusal rule ID on failure:** `smoke.assert.graphql-response`
```

The rows below are **skeletons** — the source-flow-verb cells say
`$FLOW_SRC/<TBD>` because the curator (Brian) fills them after the
first source-flow read pass. Until then, a smoke run against a row
without a source-flow citation REJECTs with `smoke.assert.no-row`.

---

## row-G1: list recent runs

**Source-flow query verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD>`

**Dagster GraphQL query:**
```graphql
query RecentRuns($limit: Int!) {
  runsOrError(limit: $limit) {
    __typename
    ... on Runs {
      results {
        runId
        status
        startTime
        endTime
      }
    }
  }
}
```

**Variables:**
```json
{ "limit": 10 }
```

**Expected response shape:**
- `data.runsOrError.__typename == "Runs"`
- `data.runsOrError.results` is an array
- Each element has the keys `runId`, `status`, `startTime`, `endTime`
- `status` is one of `STARTING`, `STARTED`, `SUCCESS`, `FAILURE`,
  `CANCELED`

**Refusal rule ID on failure:** `smoke.assert.graphql-response`

**Cross-link:** coverage `01-state-management.md` C1.

---

## row-G2: terminate a run

**Source-flow query verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD>`

**Dagster GraphQL query (mutation):**
```graphql
mutation TerminateRun($runId: String!) {
  terminateRun(runId: $runId) {
    __typename
    ... on TerminateRunSuccess { run { runId status } }
    ... on TerminateRunFailure { message }
    ... on RunNotFoundError { runId }
    ... on PythonError { message }
  }
}
```

**Variables:**
```json
{ "runId": "<RUN_ID>" }
```

**Expected response shape:**
- Either `data.terminateRun.__typename == "TerminateRunSuccess"` with
  `run.status` transitioning to `CANCELED` per coverage
  `02-stop-and-rerun.md` C1, OR
- `data.terminateRun.__typename` is one of `TerminateRunFailure` /
  `RunNotFoundError` (with `message` / `runId` populated)
- `data.terminateRun.__typename == "PythonError"` is a REJECT
  regardless of message — surface to user.

**Refusal rule ID on failure:** `smoke.assert.graphql-response`

**Cross-link:** coverage `02-stop-and-rerun.md` C1.

---

## row-G3: list code locations / repositories

**Source-flow query verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD>`

**Dagster GraphQL query:**
```graphql
query Repos {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location { name }
      }
    }
  }
}
```

**Expected response shape:**
- `__typename == "RepositoryConnection"`
- `nodes` is a non-empty array
- Each `nodes[i]` has `name` and `location.name` populated

**Refusal rule ID on failure:** `smoke.assert.graphql-response`

**Cross-link:** coverage `03-job-scheduling.md` C3 and coverage
`05-logs-and-env-status.md` C6.

---

## row-G4: launch an asset materialization

**Source-flow query verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD>`

**Dagster GraphQL query (mutation):**
```graphql
mutation MaterializeAssets($executionParams: ExecutionParams!) {
  launchPipelineExecution(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
    ... on InvalidStepError { invalidStepKey }
  }
}
```

**Variables:** an `ExecutionParams` block selecting the assets to
materialize, with `runConfigData` if needed.

**Expected response shape:**
- `data.launchPipelineExecution.__typename == "LaunchRunSuccess"`
- `run.status` is `STARTING` or `STARTED`

**Refusal rule ID on failure:** `smoke.assert.graphql-response`

**Cross-link:** coverage `01-state-management.md` C1 and coverage
`04-dependency-definition.md` C1.

---

## Adding a new row

Same rule as `cli-conformance.md`: new rows come from the curator
after a case study lands in `memory/lessons_learned/_inbox/`. The
agent does NOT add rows inline. Missing row → REJECT with
`smoke.assert.no-row`.

## Cross-personality consultation

When you need the exact Dagster GraphQL schema or sample queries
beyond what's in this file, the verify tick MAY consult:

- `personalities/dagster-expert/skills/cli-cheatsheet/SKILL.md`
  (sometimes lists `dagster-graphql` invocations)
- `personalities/dagster-expert/database/dagster-1.13.3/docs/INDEX.md`
  (for GraphQL topic entries, if curated)

The verify tick never invents query field names from training memory.
If a field isn't in the dagster-expert corpus, file a case study
rather than guessing.
