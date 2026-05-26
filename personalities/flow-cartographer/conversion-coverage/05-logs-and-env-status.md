<!-- all-might generated -->
# Audit: 05 — Logs & Environment Status

Scope: AP's logging model and environment-health surface versus
Dagster 1.13.3's event log, compute log manager, structured Pipes
logging, `dagster instance info` / `dagster instance migrate`, and
`dagster-daemon liveness-check`.

## AP behavior (must cite from $AP_SRC)

Required reading paths (use `grep -rn "<keyword>" $AP_SRC`):
- Log emit / collect module (search `log`, `logger`, `event`)
- Compute / step output capture (search `stdout`, `stderr`, `capture`,
  `tee`)
- Migration / schema-management module (search `migrate`, `schema`,
  `upgrade`, `version`)
- Health probe / liveness module (search `health`, `live`, `ready`,
  `ping`)
- Env / config surfacing (search `env`, `environ`, `instance`,
  `config`)

Expected behaviors (AP-side, to be confirmed by `$AP_SRC` reading):
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

## Parity criteria (PASS only if ALL true)

- [ ] C1: AP structured-event types are mapped onto Dagster event
  types (`STEP_START`, `STEP_FAILURE`, `STEP_RETRY`, `STEP_SUCCESS`,
  `RUN_FAILURE`, …) row by row.
- [ ] C2: AP stdout/stderr capture is mapped onto
  `LocalComputeLogManager` (or another `compute_logs` choice) in
  `dagster.yaml`. Path layout shown.
- [ ] C3: Structured logging from external scripts is mapped onto
  `dagster_pipes` (`pipes.log.{info,warning,error}` and
  `pipes.report_asset_materialization`). AP's script wrapper changes
  are listed.
- [ ] C4: AP schema migrations are mapped onto `dagster instance
  migrate`, with the upgrade order documented (which dagster version
  introduces which migration).
- [ ] C5: AP liveness probe is mapped onto `dagster-daemon
  liveness-check` (or an explicit gap), with the exit-code contract
  named.
- [ ] C6: AP "current env status" inspection verb is mapped onto
  `dagster instance info`, with the columns/fields shown in the
  expected output.
- [ ] C7: A debug-export path is documented for offline forensics
  (`dagster debug export <RUN_ID>` + `dagster-webserver-debug`).

## Refusal triggers (mechanical)

- C1 unmet → `REJECT: 05.C1: AP events not mapped row-by-row onto
  Dagster event types. Remediation: list each AP event with its
  Dagster counterpart, cite learn/05-failures/README.md.`
- C2 unmet → `REJECT: 05.C2: compute log capture not mapped onto a
  compute_logs choice. Remediation: cite
  skills/dagster-yaml-reference/SKILL.md::compute_logs and pick a
  manager.`
- C3 unmet → `REJECT: 05.C3: external-script logging not mapped onto
  dagster_pipes. Remediation: cite learn/09-real-flow/README.md and
  list the wrapper changes per AP script type.`
- C4 unmet → `REJECT: 05.C4: AP migrations not mapped onto dagster
  instance migrate. Remediation: cite
  skills/cli-cheatsheet/SKILL.md::instance migrate and document the
  upgrade order.`
- C5 unmet → `REJECT: 05.C5: AP liveness not mapped onto dagster-daemon
  liveness-check or not flagged as gap. Remediation: name the exit-code
  contract and the probe interval.`
- C6 unmet → `REJECT: 05.C6: env-status inspection verb not mapped
  onto dagster instance info. Remediation: cite the CLI and list the
  expected output columns.`
- C7 unmet → `REJECT: 05.C7: no debug-export path documented.
  Remediation: cite skills/cli-cheatsheet/SKILL.md::debug export and
  dagster-webserver-debug.`

## Evidence template

| Criterion | AP source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $AP_SRC/... | learn/05-failures/README.md::event types | PASS / FAIL |
| C2 | $AP_SRC/... | skills/dagster-yaml-reference/SKILL.md::compute_logs | PASS / FAIL |
| C3 | $AP_SRC/... | learn/09-real-flow/README.md::dagster_pipes | PASS / FAIL |
| C4 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::instance migrate | PASS / FAIL |
| C5 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::liveness-check | PASS / FAIL |
| C6 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::instance info | PASS / FAIL |
| C7 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::debug export | PASS / FAIL |
