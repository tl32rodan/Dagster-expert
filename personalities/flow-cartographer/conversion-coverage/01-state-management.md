<!-- all-might generated -->
# Coverage: 01 — State Management

Scope: the source flow's run / asset / partition state model and its
storage backends, versus Dagster 1.13.3's DagsterRun, event log, run
storage, schedule storage, and partition state.

## Flow behavior (must cite from $FLOW_SRC)

Required reading paths (use `grep -rn "<keyword>" $FLOW_SRC`):
- Run / job state machine module (search `state`, `status`, `STARTED`,
  `SUCCESS`, `FAILURE`, `CANCELED`)
- Persistence / storage layer (search `db`, `sqlite`, `postgres`,
  `store`)
- Partition / unit-of-work module if the flow partitions runs by cell /
  branch / step / PVT (search `partition`, `branch`, `step`)
- Asset / output staleness model if the flow tracks materialization
  (search `materialize`, `version`, `stale`)

Expected behaviors (flow-side, to be confirmed by `$FLOW_SRC` reading):
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

## Coverage criteria (covered only if ALL true)

- [ ] C1: The flow's unit-of-work statuses are mapped onto specific
  `DagsterRunStatus` values, with a citation row per flow status →
  Dagster status pair.
- [ ] C2: The flow's persistence layer is mapped onto one of
  `dagster.yaml` `run_storage` / `event_log_storage` /
  `schedule_storage` configurations (or a documented gap if Dagster
  cannot represent it).
- [ ] C3: The flow's partial completion semantics are representable in
  Dagster's partition state — the increment names which
  `PartitionsDefinition` (or `MultiPartitions`) shape encodes which flow
  sub-unit, and the cardinality math is shown.
- [ ] C4: The flow's rerun-from-state semantics are mapped onto either
  Dagster re-execute, re-execute-from-failure, or a documented
  checkpoint pattern (see coverage `02-stop-and-rerun.md` for the rerun
  surface in detail).
- [ ] C5: The flow's staleness signal (if any) is mapped onto
  `DataVersion` / `MaterializeResult(data_version=...)`. If the flow
  uses clock-time staleness, the increment flags this as an
  intentional gap.
- [ ] C6: Storage backend choice (SQLite vs Postgres) is justified by
  total leaf cardinality (branches × steps × cells × PVTs × …) per
  the user pref "cardinality math first" in MEMORY.md.

## Gap triggers (mechanical)

Each criterion is **covered** (the increment cites the mapping) or a
**gap**. An unaddressed gap is a `coverage-gap` finding (verify check 6
FAILs); a gap explicitly parked in `flow-model/_open_questions.yaml` is
acceptable, not a hard reject. Each remediation below is how to *cover*
the criterion — parking it as an open question is the documented
alternative.

- C1 gap → `coverage-gap 01.C1: flow status set is not mapped row-by-row
  to DagsterRunStatus. Remediation: add a table with one row per flow
  status with $FLOW_SRC/<file>:<line> + DagsterRunStatus value.`
- C2 gap → `coverage-gap 01.C2: flow persistence layer not mapped onto
  dagster.yaml. Remediation: cite the run_storage/event_log_storage
  config block in personalities/dagster-expert/skills/dagster-yaml-reference/SKILL.md.`
- C3 gap → `coverage-gap 01.C3: flow partial-completion semantics not
  encoded as a PartitionsDefinition. Remediation: show cardinality
  math + name the partition shape (Static / Dynamic / Multi).`
- C4 gap → `coverage-gap 01.C4: flow rerun semantics not mapped onto
  Dagster re-execute / re-execute-from-failure / checkpoint pattern.
  Remediation: cross-link to coverage 02-stop-and-rerun.md row.`
- C5 gap → `coverage-gap 01.C5: flow staleness signal not mapped or not
  flagged as gap. Remediation: either cite DataVersion mapping or
  flag the gap explicitly.`
- C6 gap → `coverage-gap 01.C6: storage backend choice not justified by
  cardinality math. Remediation: enumerate the leaf count (branches
  × steps × cells × PVTs × …) before naming SQLite or Postgres.`

## Evidence template

| Criterion | Flow source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $FLOW_SRC/... | database/dagster-1.13.3/docs/<file>.md::... | covered / gap |
| C2 | $FLOW_SRC/... | skills/dagster-yaml-reference/SKILL.md::... | covered / gap |
| C3 | $FLOW_SRC/... | database/dagster-1.13.3/docs/partitions.md::... | covered / gap |
| C4 | $FLOW_SRC/... | (cross-link to coverage 02) | covered / gap |
| C5 | $FLOW_SRC/... | database/dagster-1.13.3/docs/data-version-and-staleness.md::... | covered / gap |
| C6 | (cardinality math) | learn/12-scaling/README.md::... | covered / gap |
