<!-- all-might generated -->
# Audit: 02 — Stop & Rerun Mechanism

Scope: AP's cancel / kill / restart / resume model versus Dagster
1.13.3's `dagster run terminate`, re-execute, re-execute-from-failure,
`RetryPolicy`, and checkpoint marker patterns (`.done` / Pipes).

## AP behavior (must cite from $AP_SRC)

Required reading paths (use `grep -rn "<keyword>" $AP_SRC`):
- Run-control / lifecycle module (search `cancel`, `terminate`,
  `abort`, `kill`)
- Resume / rerun logic (search `resume`, `restart`, `rerun`, `retry`,
  `checkpoint`)
- Done-marker pattern, if AP uses filesystem markers (search `.done`,
  `done_marker`, `complete`)
- Daemon / supervisor module — what happens when the daemon restarts
  mid-flight (search `daemon`, `supervisor`, `liveness`, `recover`)

Expected behaviors (AP-side, to be confirmed by `$AP_SRC` reading):
- B1: A running unit-of-work can be cancelled from outside (UI / CLI
  / API) and reaches a final state that distinguishes "cancelled"
  from "failed".
- B2: A killed process (SIGKILL or daemon restart) leaves the
  unit-of-work in a recoverable state (orphan detection, or status
  upgrade to FAILED).
- B3: A re-run can be either "full retry" or "from-failure" /
  "from-checkpoint" — at least one of these is supported by the AP
  surface.
- B4: Checkpointing (if any) is at a known granularity (per step /
  per partition / per cell) and uses an explicit marker the next run
  can read.
- B5: Retry policies are configurable per work unit (max retries,
  backoff).

## Dagster 1.13.3 corresponding API

Source:
`personalities/dagster-expert/database/dagster-1.13.3/docs/failures-retries.md`
Also see:
- `personalities/dagster-expert/learn/06-interrupt-rerun/README.md`
  (top-level)
- `personalities/dagster-expert/learn/06-interrupt-rerun/6a-cancel/`
- `personalities/dagster-expert/learn/06-interrupt-rerun/6b-killed/`
- `personalities/dagster-expert/learn/06-interrupt-rerun/6c-restart/`
- `personalities/dagster-expert/learn/06-interrupt-rerun/6d-checkpoint/`
- `personalities/dagster-expert/skills/diagnose-orphan-run/SKILL.md`

Public APIs / classes / CLI commands:
- `dagster run terminate <RUN_ID>` — cite `skills/cli-cheatsheet/SKILL.md`
- Re-execute / Re-execute from failure (UI + CLI) — cite
  `learn/06-interrupt-rerun/README.md`
- `RetryPolicy(max_retries=N, delay=...)` — cite
  `docs/failures-retries.md`
- Checkpoint pattern: `.done` markers via dagster_pipes; the asset
  reads existing markers before re-doing work — cite `learn/06-…/6d-checkpoint/`
- Orphan-run recovery: `skills/diagnose-orphan-run/SKILL.md`

## Parity criteria (PASS only if ALL true)

- [ ] C1: AP cancel/abort verb maps onto `dagster run terminate
  <RUN_ID>`; the resulting Dagster state is documented as `CANCELED`
  (distinct from FAILURE) per audit `01-state-management.md`.
- [ ] C2: AP kill / daemon-restart behavior is mapped onto Dagster's
  orphan-run recovery semantics, with reference to
  `skills/diagnose-orphan-run/SKILL.md`. The plan names which Dagster
  configuration ensures the same recovery posture.
- [ ] C3: AP "re-run" verb is mapped onto Dagster's "Re-execute" (full
  retry) **or** "Re-execute from failure" (cost-optimized), with the
  choice justified by AP's semantics.
- [ ] C4: If AP uses checkpoints, the migration plan maps the AP
  checkpoint granularity onto either (a) Dagster's
  `RetryPolicy`-driven step retries, **or** (b) the `.done` marker
  pattern from `learn/06-…/6d-checkpoint/`, **or** (c) an explicit gap
  if neither fits.
- [ ] C5: AP retry-policy parameters (max retries, backoff) are mapped
  onto `RetryPolicy(max_retries=..., delay=...)` arguments.
- [ ] C6: The plan distinguishes "user cancel" (intentional → CANCELED)
  from "killed by infra" (unintentional → orphan → FAILURE) and shows
  how each AP path lands in the correct Dagster state.

## Refusal triggers (mechanical)

- C1 unmet → `REJECT: 02.C1: AP cancel verb not mapped to dagster run
  terminate or resulting state ambiguous. Remediation: cite the AP
  cancel call site $AP_SRC/<file>:<line> and the Dagster terminate
  command in skills/cli-cheatsheet/SKILL.md.`
- C2 unmet → `REJECT: 02.C2: AP kill / daemon-restart not mapped onto
  orphan-recovery posture. Remediation: cross-link to
  skills/diagnose-orphan-run/SKILL.md and cite the AP supervisor
  module.`
- C3 unmet → `REJECT: 02.C3: AP re-run not explicitly mapped to either
  full retry or from-failure. Remediation: state which Dagster
  re-execute mode applies and why.`
- C4 unmet → `REJECT: 02.C4: AP checkpoint granularity not mapped or
  not flagged as gap. Remediation: cite learn/06-…/6d-checkpoint/ or
  RetryPolicy, or write the gap explicitly.`
- C5 unmet → `REJECT: 02.C5: AP retry parameters not mapped to
  RetryPolicy args. Remediation: cite docs/failures-retries.md with
  the AP parameter names alongside the Dagster ones.`
- C6 unmet → `REJECT: 02.C6: user-cancel vs infra-kill not
  distinguished in the plan. Remediation: add a row per source of
  termination with the resulting Dagster state.`

## Evidence template

| Criterion | AP source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $AP_SRC/... | skills/cli-cheatsheet/SKILL.md::run terminate | PASS / FAIL |
| C2 | $AP_SRC/... | skills/diagnose-orphan-run/SKILL.md::... | PASS / FAIL |
| C3 | $AP_SRC/... | learn/06-interrupt-rerun/README.md::... | PASS / FAIL |
| C4 | $AP_SRC/... | learn/06-…/6d-checkpoint/ or docs/failures-retries.md | PASS / FAIL |
| C5 | $AP_SRC/... | docs/failures-retries.md::RetryPolicy | PASS / FAIL |
| C6 | $AP_SRC/... | (audit 01 + this audit) | PASS / FAIL |
