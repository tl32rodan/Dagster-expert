---
name: verify-loop
description: >
  Lightweight self-check gate on the just-built increment, with a
  DIFFERENT framing from the builder. Promotes `built` → `done`, or
  sends it to `blocked` with a finding. Runs on the `verify` tick
  (fires :30, after the :00 build). This is the verify-gate; it is not
  the old strict acceptance audit.
---

# verify-loop — gate one built increment

The builder asks "how do I convert this?" You ask the opposite: **"what
would make this wrong?"** Re-read the increment's output cold — do not
trust the build journal's claims; check the files yourself.

## Step 1 — Pick the increment

From `_plan.yaml`, the increment with `status: built` (normally
`STATUS.md::active_increment`). If more than one is `built`, take the
lowest id. If none is `built`, write a `noop` handoff and stop.

## Step 2 — Run the six checks (mechanical; any FAIL → blocked)

Run all six; record each as PASS / FAIL with one line of evidence.

1. **API exists in corpus.** `grep -nE '^\s*(from|import) dagster'` the
   increment's files. For each symbol, confirm it appears in
   `personalities/dagster-expert/database/dagster-1.13.3/docs/` (use the
   sibling `lookup-api` search order). Any 0-result symbol →
   FAIL `invented-api`.
2. **No private imports.** `grep -nE 'dagster\._(core|internal|private)'`
   → any hit → FAIL `private-import`.
3. **Smoke runs.** Run the increment's `accept` command (e.g.
   `python -m _smoke`, or `dagster asset materialize -m <mod> --partition
   '<k>'` with absolute `-w`). Capture exit code + tail of output. Exit
   ≠ 0 → FAIL `smoke-failed` (paste the failing output into the finding).
4. **Converted, not copied.** Diff the increment's output against its
   `converts:` source. If a source file was copied in essentially
   unchanged (no Dagster asset / partition / config-generator wrapping
   it), → FAIL `not-converted`. For an L0 increment, confirm a
   config file + generator exist and the generator reproduces the
   per-leaf files (don't just trust it — run it).
5. **Source cited.** The build journal entry names `$FLOW_SRC/<file>:<line>`
   and the Dagster API used. Missing → FAIL `uncited`.
6. **Coverage preserved.** Open the relevant `conversion-coverage/0N-*.md`
   for the step's concern (state / stop&rerun / scheduling / deps /
   logs). Confirm the conversion preserves that behavior OR the gap is
   already parked in `_open_questions.yaml`. Unaddressed gap →
   FAIL `coverage-gap`.

Use `standards/refusal-patterns.md` for the finding format.

## Step 3 — Verdict (binary, per increment)

- **All six PASS** → set the increment `status: done`. Commit the
  increment's files locally (small, scoped commit; do not push). Journal
  `verified <id>: pass`.
- **Any FAIL** → set the increment `status: blocked`. Write a finding to
  `flow-model/_open_questions.yaml` under `findings:`:
  ```yaml
  - increment: a3
    date: YYYY-MM-DD
    type: invented-api      # one of the FAIL codes above
    detail: "<one line: what, where>"
    fix_hint: "<one line: what the next build must do>"
  ```
  The next `build` tick re-picks this increment (it is no longer
  `done`) and fixes it before advancing. Do NOT best-effort past a
  FAIL; do NOT mark it done "with caveats".

## Step 4 — Handoff and stop

Per Wake SOP Step 4: append `_operations.log`
(`<ts> verify <id> <pass|blocked> <code-or-'-'>`), update `STATUS.md`
(`next_action`: on pass → `build <next ready increment>`; on block →
`build <id> (fix <fail-code>)`), journal the verdict with evidence.
One increment, then stop.

> Note: this gate is deliberately *lightweight* compared with the
> retired strict gatekeeper. It exists to catch the weak-model failure
> modes (invented API, blind copy, doesn't run), not to adjudicate a
> migration plan. The 5 `conversion-coverage/` aspects are a checklist
> here, not a PASS/REJECT product.
