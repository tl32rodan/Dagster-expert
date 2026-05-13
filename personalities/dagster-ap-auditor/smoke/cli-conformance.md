<!-- all-might generated -->
# CLI conformance — AP CLI ↔ Dagster CLI mapping (skeleton)

Each row maps an AP CLI verb onto a Dagster 1.13.3 CLI invocation
and declares per-assertion expectations. The auditor reads one row
per SMOKE invocation, runs the Dagster command, and diffs against
the row.

## How to read a row

```
## <row-id>: <human title>

**AP verb (cite from $AP_SRC):**
  $AP_SRC/<file>:<line>  →  `<ap cli string>`

**Dagster command (air-gap, absolute paths):**
  dagster <subcommand> -w /abs/path/to/workspace.yaml ...

**Expected exit code:** 0 / 1 / …

**Expected stdout shape:**
  ```regex / excerpt```

**Wall-time bound:** ≤ N seconds (optional)

**Refusal rule ID on failure:** smoke.assert.<id>
```

The rows below are **skeletons** — the AP-verb cells say
`$AP_SRC/<TBD>` because the curator (Brian) fills them after the
first AP read pass. Until then, an attempt to run a row REJECTs with
`smoke.assert.no-row` (no matching AP behavior recorded yet).

---

## row-01: terminate a running job

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP cancel/abort verb>`

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

**Cross-link:** audit `02-stop-and-rerun.md` C1.

---

## row-02: list active schedules

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP "list schedules" verb>`

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

**Cross-link:** audit `03-job-scheduling.md` C5.

---

## row-03: inspect instance / env status

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP "instance info" / "status" verb>`

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

**Cross-link:** audit `05-logs-and-env-status.md` C6.

---

## row-04: instance schema migrate

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP "schema migrate" verb>`

**Dagster command:**
```
dagster instance migrate
```

**Expected exit code:** 0

**Expected stdout shape:** `.*Migrating .* schema.*` (or "No
migration needed").

**Wall-time bound:** ≤ 60 s

**Refusal rule ID on failure:** `smoke.assert.exit-code`

**Cross-link:** audit `05-logs-and-env-status.md` C4.

**Warning:** this row is destructive on the live event log /
run-storage schema. The auditor MUST refuse to run row-04 against a
production `DAGSTER_HOME` without explicit "yes go ahead"
confirmation from the user.

---

## row-05: daemon liveness probe

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP daemon liveness/health verb>`

**Dagster command:**
```
dagster-daemon liveness-check
```

**Expected exit code:** 0 (healthy) or non-zero (unhealthy).

**Expected stdout shape:** Minimal; the contract is the exit code.

**Wall-time bound:** ≤ 5 s

**Refusal rule ID on failure:** `smoke.assert.exit-code`

**Cross-link:** audit `05-logs-and-env-status.md` C5.

---

## row-06: backfill a partition range

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP backfill verb>`

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

**Cross-link:** audit `03-job-scheduling.md` C4.

---

## row-07: debug-export a run

**AP verb (cite from $AP_SRC):** `$AP_SRC/<TBD>:<TBD>` →
`<TBD: AP run-forensics export verb>`

**Dagster command:**
```
dagster debug export <RUN_ID> /tmp/run-<RUN_ID>.gz
```

**Expected exit code:** 0

**Expected stdout shape:** `.*Exported run to .*\.gz`

**Wall-time bound:** ≤ 30 s for small runs.

**Refusal rule ID on failure:** `smoke.assert.exit-code`

**Cross-link:** audit `05-logs-and-env-status.md` C7.

---

## Adding a new row

New rows come from the curator after a case study lands in
`memory/lessons_learned/_inbox/`. The agent does NOT add rows
inline during a SMOKE invocation. If a SMOKE request has no matching
row, REJECT with:

```
REJECT: smoke.assert.no-row:
  <ap-verb>: no matching row in smoke/cli-conformance.md
  Remediation: file a case study at
    personalities/dagster-ap-auditor/memory/lessons_learned/_inbox/<ISO>-<unix_user>.md
    describing the AP verb and a proposed Dagster CLI mapping
  Source: personalities/dagster-ap-auditor/smoke/cli-conformance.md (rows are curator-edit only)
```
