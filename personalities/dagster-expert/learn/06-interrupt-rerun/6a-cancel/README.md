# 6a · cancel a run mid-execution

**Time**: 15 min

## Setup

This lab gives you an asset that takes 30 seconds to materialize
(via `time.sleep`). You'll launch it, cancel it from the UI, and
observe what Dagster does.

```bash
cd ~/projects/dagster-lab/lab6-interrupt-rerun/6a-cancel
dagster dev -m slow
# UI: http://127.0.0.1:3000
```

## Walkthrough

### Step 1 · Materialize

Click **Materialize** on the `slow_payload` asset.

The run launches. In the **Runs** tab, click the new run. Status:
`STARTED`.

### Step 2 · While it's still running, click "Terminate"

Top-right of the run detail page. Confirm in the popup.

### Step 3 · Observe

What's the run status now?

> **Expected**: `CANCELED` after a brief `CANCELING` window.
>
> What just happened in process terms: Dagster sent SIGTERM to the
> run worker. The worker had `KeyboardInterrupt` raised inside the
> asset function (Dagster catches the signal, translates to
> Python-level interrupt). The asset function exited mid-`sleep`.

### Step 4 · Check the asset

Go back to the asset detail. Look at "Materializations". Is there a
new materialization?

> **Expected**: NO. The run was canceled before
> `MaterializeResult` was returned. The asset is still in whatever
> state it was before the run.

### Step 5 · Re-execute

The Run detail page has a "↻ Re-execute" button (top-right). Click
it.

> **Expected**: a new run launches, runs for ~30s, succeeds. Asset
> is now materialized.

> Note: there's no "Re-execute from failure" for a canceled run —
> cancel doesn't capture partial state in a way Dagster can
> resume from. You always re-run from scratch. (See sub-lab 6d for
> the workaround if a 30-second sleep was actually 30 minutes of
> useful work.)

## Now try

### Try 1 · Cancel right after launch (within 1 second)

Does Dagster catch it? Sometimes the worker is still spinning up,
not in your code yet.

> **Expected**: still `CANCELED`. Dagster's run launcher catches the
> termination request before subprocess spawn.

### Try 2 · Look at run logs after cancel

In the run detail, scroll the events. You should see:
- `RUN_STARTED`
- `RUN_CANCELING`
- `RUN_CANCELED`

The asset's own `STEP_START` may or may not be there depending on
how far it got.

### Try 3 · Cancel via CLI

```bash
# In another terminal
dagster run terminate <run_id>
```

`<run_id>` is visible in the UI's run URL. Same effect as the
button.

## Common pitfalls

- **"Cancel" hung on CANCELING**: usually means the worker is in a
  C-extension or syscall that doesn't respond to SIGTERM. Wait
  ~30s — Dagster escalates to SIGKILL. Or proceed to sub-lab 6b.
- **Run shows SUCCESS but you canceled**: race condition — your
  cancel arrived after the asset returned. Refresh.
- **Re-execute spawns a NEW run id**: yes, by design. Each run is
  immutable history; re-execute is a new run with a `parent_run_id`
  pointer back.
