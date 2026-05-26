<!-- all-might generated -->
# Coverage: 05 — Logs & Environment Status

Scope: the source flow's logging model and environment-health surface
versus Dagster 1.13.3's event log, compute log manager, structured
Pipes logging, `dagster instance info` / `dagster instance migrate`,
and `dagster-daemon liveness-check`.

## Flow behavior (must cite from $FLOW_SRC)

Required reading paths (use `grep -rn "<keyword>" $FLOW_SRC`):
- Log emit / collect module (search `log`, `logger`, `event`)
- Compute / step output capture (search `stdout`, `stderr`, `capture`,
  `tee`)
- Migration / schema-management module (search `migrate`, `schema`,
  `upgrade`, `version`)
- Health probe / liveness module (search `health`, `live`, `ready`,
  `ping`)
- Env / config surfacing (search `env`, `environ`, `instance`,
  `config`)

Expected behaviors (flow-side, to be confirmed by `$FLOW_SRC` reading):
- B1: Step / run events are emitted as structured records (not just
  free-form text) and persisted.
- B2: stdout / stderr of each step is captured and accessible after
  the step finishes.
- B3: Schema migrations are explicit — an upgrade path exists from
  one schema version to the next, with a CLI verb.
- B4: A liveness probe exists for the long-lived daemon process,
  returning a machine-readable status.
- B5: The "current environment status" of an instance (which DB,
  which storage, which schemas) is inspectable via a single command.

## Dagster 1.13.3 corresponding API

Source:
- `personalities/dagster-expert/learn/05-failures/README.md` (event
  log structure: STEP_START, STEP_FAILURE, STEP_RETRY, STEP_SUCCESS,
  RESOURCE_TEARDOWN, RUN_FAILURE)
- `personalities/dagster-expert/learn/12-scaling/README.md` (event
  log scaling + migration)
- `personalities/dagster-expert/learn/09-real-flow/README.md`
  (dagster_pipes structured logging from external scripts)

Also see:
- `personalities/dagster-expert/skills/cli-cheatsheet/SKILL.md`
  (`dagster instance info`, `dagster instance migrate`,
  `dagster-daemon liveness-check`, `dagster debug export`)
- `personalities/dagster-expert/skills/verify-deploy/SKILL.md`
- `personalities/dagster-expert/skills/dagster-yaml-reference/SKILL.md`
  (`compute_logs` stanza: LocalComputeLogManager, etc.)

Public APIs / classes / CLI commands:
- Event types: `STEP_START`, `STEP_FAILURE`, `STEP_RETRY`,
  `STEP_SUCCESS`, `RUN_FAILURE`, etc. — cite `learn/05-failures/README.md`
- `dagster_pipes`: `pipes.log.info(...)`, `pipes.log.error(...)`,
  `pipes.report_asset_materialization(data_version=...,
  metadata=...)` — cite `learn/09-real-flow/README.md`
- `dagster.yaml::compute_logs` stanza: `LocalComputeLogManager` — cite
  `skills/dagster-yaml-reference/SKILL.md`
- CLI: `dagster instance info`, `dagster instance migrate` — cite
  `skills/cli-cheatsheet/SKILL.md`
- CLI: `dagster-daemon liveness-check` — cite
  `skills/cli-cheatsheet/SKILL.md`
- CLI: `dagster debug export <RUN_ID>` for offline run forensics —
  cite `skills/cli-cheatsheet/SKILL.md`

## Coverage criteria (covered only if ALL true)

- [ ] C1: The flow's structured-event types are mapped onto Dagster
  event types (`STEP_START`, `STEP_FAILURE`, `STEP_RETRY`,
  `STEP_SUCCESS`, `RUN_FAILURE`, …) row by row.
- [ ] C2: The flow's stdout/stderr capture is mapped onto
  `LocalComputeLogManager` (or another `compute_logs` choice) in
  `dagster.yaml`. Path layout shown.
- [ ] C3: Structured logging from external scripts is mapped onto
  `dagster_pipes` (`pipes.log.{info,warning,error}` and
  `pipes.report_asset_materialization`). The flow's script wrapper
  changes are listed.
- [ ] C4: The flow's schema migrations are mapped onto `dagster
  instance migrate`, with the upgrade order documented (which dagster
  version introduces which migration).
- [ ] C5: The flow's liveness probe is mapped onto `dagster-daemon
  liveness-check` (or an explicit gap), with the exit-code contract
  named.
- [ ] C6: The flow's "current env status" inspection verb is mapped
  onto `dagster instance info`, with the columns/fields shown in the
  expected output.
- [ ] C7: A debug-export path is documented for offline forensics
  (`dagster debug export <RUN_ID>` + `dagster-webserver-debug`).

## Gap triggers (mechanical)

Each criterion is **covered** (the increment cites the mapping) or a
**gap**. An unaddressed gap is a `coverage-gap` finding (verify check 6
FAILs); a gap explicitly parked in `flow-model/_open_questions.yaml` is
acceptable, not a hard reject. Each remediation below is how to *cover*
the criterion — parking it as an open question is the documented
alternative.

- C1 gap → `coverage-gap 05.C1: flow events not mapped row-by-row onto
  Dagster event types. Remediation: list each flow event with its
  Dagster counterpart, cite learn/05-failures/README.md.`
- C2 gap → `coverage-gap 05.C2: compute log capture not mapped onto a
  compute_logs choice. Remediation: cite
  skills/dagster-yaml-reference/SKILL.md::compute_logs and pick a
  manager.`
- C3 gap → `coverage-gap 05.C3: external-script logging not mapped onto
  dagster_pipes. Remediation: cite learn/09-real-flow/README.md and
  list the wrapper changes per flow script type.`
- C4 gap → `coverage-gap 05.C4: flow migrations not mapped onto dagster
  instance migrate. Remediation: cite
  skills/cli-cheatsheet/SKILL.md::instance migrate and document the
  upgrade order.`
- C5 gap → `coverage-gap 05.C5: flow liveness not mapped onto
  dagster-daemon liveness-check or not flagged as gap. Remediation: name
  the exit-code contract and the probe interval.`
- C6 gap → `coverage-gap 05.C6: env-status inspection verb not mapped
  onto dagster instance info. Remediation: cite the CLI and list the
  expected output columns.`
- C7 gap → `coverage-gap 05.C7: no debug-export path documented.
  Remediation: cite skills/cli-cheatsheet/SKILL.md::debug export and
  dagster-webserver-debug.`

## Evidence template

| Criterion | Flow source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $FLOW_SRC/... | learn/05-failures/README.md::event types | covered / gap |
| C2 | $FLOW_SRC/... | skills/dagster-yaml-reference/SKILL.md::compute_logs | covered / gap |
| C3 | $FLOW_SRC/... | learn/09-real-flow/README.md::dagster_pipes | covered / gap |
| C4 | $FLOW_SRC/... | skills/cli-cheatsheet/SKILL.md::instance migrate | covered / gap |
| C5 | $FLOW_SRC/... | skills/cli-cheatsheet/SKILL.md::liveness-check | covered / gap |
| C6 | $FLOW_SRC/... | skills/cli-cheatsheet/SKILL.md::instance info | covered / gap |
| C7 | $FLOW_SRC/... | skills/cli-cheatsheet/SKILL.md::debug export | covered / gap |
