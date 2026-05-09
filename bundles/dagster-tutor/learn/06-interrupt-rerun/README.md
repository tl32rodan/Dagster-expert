# lab6 · interrupt + rerun handling

**Time**: 60 min · **Prerequisites**: lab1, lab3 (partitions), lab5 (failures)

> 💡 This is the lab you specifically asked for. It covers what
> happens when runs don't follow the happy path: user cancels,
> processes get killed, dagster dev restarts mid-flight, and how to
> write your own checkpoints so reruns don't redo finished work.

## What you'll learn

- Run statuses (`STARTED`, `STARTING`, `SUCCESS`, `FAILURE`,
  `CANCELED`) and what each one means for dagster
- The two distinct "Cancel" semantics in the UI
- What `dagster dev` does on restart with in-flight runs
- "Re-execute from failure" vs "Re-execute" — when to use each
- Writing your own checkpoint logic in Pipes-wrapped scripts so
  rerun doesn't redo completed work

## The lab is 4 sub-scenarios

Run each in order:

| Sub-lab | Topic | ~Time |
|---|---|---|
| `6a-cancel` | Cancel a run mid-execution | 15m |
| `6b-killed` | Process gets `kill -9`'d | 15m |
| `6c-restart` | `dagster dev` restart mid-flight | 15m |
| `6d-checkpoint` | Pipes script with self-checkpoint | 15m |

Each sub-lab has its own README. Read it, run it, observe.

## Why this matters more than you'd think

The happy-path Dagster demo always shows green checkmarks. Real
production workflows die in the middle: a Perl script segfaults
halfway through 30 partitions, a node reboots while a 4-hour run
is at hour 3, you hit the wrong button and want to undo. The
question isn't "does Dagster handle these?" (it does, mostly). The
question is **"what's the recovery path each time, and how do you
not lose 3 hours of work?"**

Each sub-lab seeds a controlled failure of one kind, makes you
observe the resulting state in the UI, then shows you the recovery
moves.

## Big-picture mental model

Dagster runs are state machines. The UI tells you what state a run
is in. The state determines:

1. What the materialize-store knows (asset is materialized? not?
   partial?)
2. What "Re-execute" buttons offer
3. Whether sibling work in the same job is salvageable

Your job as operator:

```
Run state            Operator action
-----                ---------------
SUCCESS              Nothing — move on
FAILURE              "Re-execute from failure" (cheaper) or
                     "Re-execute" (full retry)
CANCELED             "Re-execute" (cancel doesn't pre-stage retry data)
STARTED, dev-killed  Manually mark FAILURE, then re-execute
```

Each sub-lab walks one of these.

## What this lab does NOT cover

- **Production deployment retries** (retry policies on schedules).
  Use `lab5-failures` for asset-level retry policies; this lab is
  about the run-level state machine, not retry plumbing.
- **Distributed execution** (Dagster + Kubernetes / Dask / etc).
  Same state machine; transport differs.
- **Dagster Cloud sensors / alerts**. Out of scope for local dev.
