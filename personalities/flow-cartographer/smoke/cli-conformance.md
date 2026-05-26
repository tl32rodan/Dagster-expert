<!-- all-might generated -->
# CLI conformance — source-flow CLI ↔ Dagster CLI mapping (skeleton)

Each row maps a source-flow CLI verb onto a Dagster 1.13.3 CLI invocation
and declares per-assertion expectations. The verify tick reads one row
per smoke run, runs the Dagster command, and diffs against
the row.

## How to read a row

```
## <row-id>: <human title>

**Source-flow verb (cite from $FLOW_SRC):**
  $FLOW_SRC/<file>:<line>  →  `<source cli string>`

**Dagster command (air-gap, absolute paths):**
  dagster <subcommand> -w /abs/path/to/workspace.yaml ...

**Expected exit code:** 0 / 1 / …

**Expected stdout shape:**
  ```regex / excerpt```

**Wall-time bound:** ≤ N seconds (optional)

**Refusal rule ID on failure:** smoke.assert.<id>
```

The rows below are **skeletons** — the source-flow-verb cells say
`$FLOW_SRC/<TBD>` because the curator (Brian) fills them after the
first source-flow read pass. Until then, an attempt to run a row REJECTs with
`smoke.assert.no-row` (no matching source-flow behavior recorded yet).

---

## row-01: terminate a running job

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow cancel/abort verb>`

**Dagster command:**
```
dagster run terminate <RUN_ID>
```
(Reference: `personalities/dagster-expert/skills/cli-cheatsheet/SKILL.md`.)

**Expected exit code:** 0

**Expected stdout shape:**
```
.*Terminated run .* .*
```

**Wall-time bound:** ≤ 10 s

**Refusal rule ID on failure:** `smoke.assert.exit-code` /
`smoke.assert.stdout-shape` / `smoke.assert.wall-time`

**Cross-link:** coverage `02-stop-and-rerun.md` C1.

---

## row-02: list active schedules

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow "list schedules" verb>`

**Dagster command:**
```
dagster schedule list -w /abs/path/to/workspace.yaml
```

**Expected exit code:** 0

**Expected stdout shape:**
```
Repository .*
Schedule: .* \[RUNNING|STOPPED\]
```

**Wall-time bound:** ≤ 5 s

**Refusal rule ID on failure:** `smoke.assert.exit-code` /
`smoke.assert.stdout-shape`

**Cross-link:** coverage `03-job-scheduling.md` C5.

---

## row-03: inspect instance / env status

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow "instance info" / "status" verb>`

**Dagster command:**
```
dagster instance info
```

**Expected exit code:** 0

**Expected stdout shape:** must include the keys `Run storage`,
`Event log storage`, `Schedule storage`, `Compute log storage`.

**Wall-time bound:** ≤ 5 s

**Refusal rule ID on failure:** `smoke.assert.exit-code` /
`smoke.assert.stdout-shape`

**Cross-link:** coverage `05-logs-and-env-status.md` C6.

---

## row-04: instance schema migrate

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow "schema migrate" verb>`

**Dagster command:**
```
dagster instance migrate
```

**Expected exit code:** 0

**Expected stdout shape:** `.*Migrating .* schema.*` (or "No
migration needed").

**Wall-time bound:** ≤ 60 s

**Refusal rule ID on failure:** `smoke.assert.exit-code`

**Cross-link:** coverage `05-logs-and-env-status.md` C4.

**Warning:** this row is destructive on the live event log /
run-storage schema. The verify tick MUST refuse to run row-04 against a
production `DAGSTER_HOME` without explicit "yes go ahead"
confirmation from the user.

---

## row-05: daemon liveness probe

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow daemon liveness/health verb>`

**Dagster command:**
```
dagster-daemon liveness-check
```

**Expected exit code:** 0 (healthy) or non-zero (unhealthy).

**Expected stdout shape:** Minimal; the contract is the exit code.

**Wall-time bound:** ≤ 5 s

**Refusal rule ID on failure:** `smoke.assert.exit-code`

**Cross-link:** coverage `05-logs-and-env-status.md` C5.

---

## row-06: backfill a partition range

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow backfill verb>`

**Dagster command:**
```
dagster asset backfill -w /abs/path/to/workspace.yaml \
  --partition-range=<from>...<to> \
  --select="<asset selector>"
```

**Expected exit code:** 0

**Expected stdout shape:** `.*Issued backfill .*`

**Wall-time bound:** N/A (the command returns once the backfill is
**queued**, not when it finishes).

**Refusal rule ID on failure:** `smoke.assert.exit-code` /
`smoke.assert.stdout-shape`

**Cross-link:** coverage `03-job-scheduling.md` C4.

---

## row-07: debug-export a run

**Source-flow verb (cite from $FLOW_SRC):** `$FLOW_SRC/<TBD>:<TBD>` →
`<TBD: source-flow run-forensics export verb>`

**Dagster command:**
```
dagster debug export <RUN_ID> /tmp/run-<RUN_ID>.gz
```

**Expected exit code:** 0

**Expected stdout shape:** `.*Exported run to .*\.gz`

**Wall-time bound:** ≤ 30 s for small runs.

**Refusal rule ID on failure:** `smoke.assert.exit-code`

**Cross-link:** coverage `05-logs-and-env-status.md` C7.

---

## Adding a new row

New rows come from the curator after a case study lands in
`memory/lessons_learned/_inbox/`. The agent does NOT add rows
inline during a verify run. If a smoke run has no matching
row, REJECT with:

```
REJECT: smoke.assert.no-row:
  <source-flow-verb>: no matching row in smoke/cli-conformance.md
  Remediation: file a case study at
    personalities/flow-cartographer/memory/lessons_learned/_inbox/<ISO>-<unix_user>.md
    describing the source-flow verb and a proposed Dagster CLI mapping
  Source: personalities/flow-cartographer/smoke/cli-conformance.md (rows are curator-edit only)
```
