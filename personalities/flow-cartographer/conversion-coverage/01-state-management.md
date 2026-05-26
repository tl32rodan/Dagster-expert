<!-- all-might generated -->
# Audit: 01 — State Management

Scope: AP's run / asset / partition state model and its storage
backends, versus Dagster 1.13.3's DagsterRun, event log, run storage,
schedule storage, and partition state.

## AP behavior (must cite from $AP_SRC)

Required reading paths (use `grep -rn "<keyword>" $AP_SRC`):
- Run / job state machine module (search `state`, `status`, `STARTED`,
  `SUCCESS`, `FAILURE`, `CANCELED`)
- Persistence / storage layer (search `db`, `sqlite`, `postgres`,
  `store`)
- Partition / unit-of-work module if AP partitions runs by cell /
  branch / step / PVT (search `partition`, `branch`, `step`)
- Asset / output staleness model if AP tracks materialization (search
  `materialize`, `version`, `stale`)

Expected behaviors (AP-side, to be confirmed by `$AP_SRC` reading):
- B1: Each unit-of-work has a status that progresses through a fixed
  set of states (analogous to STARTED / SUCCESS / FAILURE / CANCELED).
- B2: State changes are persisted to a backing store survivable across
  process restarts.
- B3: Partial completion is representable — some sub-units succeed
  while others fail / are pending.
- B4: A re-run can resume from the last persisted state (or the user
  can force a fresh run).
- B5: Asset / output staleness (if any) is computed from a versioning
  signal, not from clock time.

## Dagster 1.13.3 corresponding API

Source:
`personalities/dagster-expert/database/dagster-1.13.3/docs/data-version-and-staleness.md`
Also see:
- `personalities/dagster-expert/learn/05-failures/README.md` (event log
  + state machine recovery)
- `personalities/dagster-expert/learn/12-scaling/README.md` (storage
  backend choices and migration)
- `personalities/dagster-expert/skills/dagster-yaml-reference/SKILL.md`
  (run_storage / event_log_storage / schedule_storage stanzas)

Public APIs / classes / CLI commands (DO NOT invent — cite the file):
- `DagsterRunStatus` enum: `STARTING`, `STARTED`, `SUCCESS`, `FAILURE`,
  `CANCELED` — cite `database/dagster-1.13.3/docs/<file>.md` line
- `DataVersion`, `MaterializeResult(data_version=...)` — cite
  `data-version-and-staleness.md`
- `dagster.yaml::run_storage`, `event_log_storage`, `schedule_storage`
  — cite `skills/dagster-yaml-reference/SKILL.md`
- CLI: `dagster instance info`, `dagster instance migrate` — cite
  `skills/cli-cheatsheet/SKILL.md`
- Partition state via `PartitionsDefinition` / `MultiPartitions` —
  cite `database/dagster-1.13.3/docs/partitions.md`

## Parity criteria (PASS only if ALL true)

- [ ] C1: AP unit-of-work statuses are mapped onto specific
  `DagsterRunStatus` values, with a citation row per AP status →
  Dagster status pair.
- [ ] C2: AP's persistence layer is mapped onto one of `dagster.yaml`
  `run_storage` / `event_log_storage` / `schedule_storage`
  configurations (or a documented gap if Dagster cannot represent it).
- [ ] C3: AP partial completion semantics are representable in
  Dagster's partition state — the migration plan names which
  `PartitionsDefinition` (or `MultiPartitions`) shape encodes which AP
  sub-unit, and the cardinality math is shown.
- [ ] C4: AP rerun-from-state semantics are mapped onto either
  Dagster re-execute, re-execute-from-failure, or a documented
  checkpoint pattern (see audit `02-stop-and-rerun.md` for the rerun
  surface in detail).
- [ ] C5: AP staleness signal (if any) is mapped onto
  `DataVersion` / `MaterializeResult(data_version=...)`. If AP uses
  clock-time staleness, the migration plan flags this as an
  intentional gap.
- [ ] C6: Storage backend choice (SQLite vs Postgres) is justified by
  total leaf cardinality (branches × steps × cells × PVTs × …) per
  the user pref "cardinality math first" in MEMORY.md.

## Refusal triggers (mechanical)

- C1 unmet → `REJECT: 01.C1: AP status set is not mapped row-by-row
  to DagsterRunStatus. Remediation: add a table with one row per AP
  status with $AP_SRC/<file>:<line> + DagsterRunStatus value.`
- C2 unmet → `REJECT: 01.C2: AP persistence layer not mapped onto
  dagster.yaml. Remediation: cite the run_storage/event_log_storage
  config block in personalities/dagster-expert/skills/dagster-yaml-reference/SKILL.md.`
- C3 unmet → `REJECT: 01.C3: AP partial-completion semantics not
  encoded as a PartitionsDefinition. Remediation: show cardinality
  math + name the partition shape (Static / Dynamic / Multi).`
- C4 unmet → `REJECT: 01.C4: AP rerun semantics not mapped onto
  Dagster re-execute / re-execute-from-failure / checkpoint pattern.
  Remediation: cross-link to audit 02-stop-and-rerun.md row.`
- C5 unmet → `REJECT: 01.C5: AP staleness signal not mapped or not
  flagged as gap. Remediation: either cite DataVersion mapping or
  flag the gap explicitly.`
- C6 unmet → `REJECT: 01.C6: storage backend choice not justified by
  cardinality math. Remediation: enumerate the leaf count (branches
  × steps × cells × PVTs × …) before naming SQLite or Postgres.`

## Evidence template

| Criterion | AP source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $AP_SRC/... | database/dagster-1.13.3/docs/<file>.md::... | PASS / FAIL |
| C2 | $AP_SRC/... | skills/dagster-yaml-reference/SKILL.md::... | PASS / FAIL |
| C3 | $AP_SRC/... | database/dagster-1.13.3/docs/partitions.md::... | PASS / FAIL |
| C4 | $AP_SRC/... | (cross-link to audit 02) | PASS / FAIL |
| C5 | $AP_SRC/... | database/dagster-1.13.3/docs/data-version-and-staleness.md::... | PASS / FAIL |
| C6 | (cardinality math) | learn/12-scaling/README.md::... | PASS / FAIL |
