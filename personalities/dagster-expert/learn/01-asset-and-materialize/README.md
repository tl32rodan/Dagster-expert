# lab1 · hello — one asset, observed

**Time**: 30 min · **Prerequisites**: nothing

## What you'll learn

- What an `@asset` actually IS at runtime
- The "Materialize" button — what runs when you click it
- Where Dagster stores the result and how to peek
- The data version concept (and why it matters even with one asset)

## The lab in 60 seconds

```bash
cd ~/projects/dagster-lab/lab1-hello
dagster dev -m hello
# open http://127.0.0.1:3000
```

You should see one asset called `greeting` in the Assets tab.

Click **Materialize** in the top right.

You should see a new run start, finish in <1s, with a green check
beside `greeting`. Click `greeting` to see metadata, including a
`data_version`.

## Now try

### Try 1 · Click Materialize again

What changes?

> Answer (don't peek): the run launches but Dagster reports the asset
> is NOT stale — same data version. The materialization itself runs
> (Dagster doesn't skip on Materialize-button click), but the
> `data_version` is identical because the asset's body produced the
> same bytes.

### Try 2 · Edit `hello/asset.py` so it returns a new string

Change the `payload` literal to anything different. Save. The UI
should refresh (you may need the "↺ Reload" button on the code
location). Click Materialize.

> What changed: data_version is now different. Look at the asset's
> Materializations tab — you'll see the version timeline.

### Try 3 · Inspect the run

In the UI, click **Runs** tab. Click your most recent run. You'll see:
- A timeline of events (start, materialize, success)
- Logs (the agent's `context.log.info(...)` lines if any)
- The materialized asset's metadata

This is the same view you'll use to debug any pipeline.

## What's actually happening

`@asset` decorates a Python function. The decorator turns it into an
`AssetsDefinition` — a thing Dagster's UI / scheduler / lineage
engine can reason about.

When you click Materialize:
1. Dagster builds an "asset job" containing just `greeting`.
2. It launches a run of that job.
3. The run executes `greeting()` in a subprocess.
4. The function's return value (a `MaterializeResult`) carries the
   data_version + metadata, which Dagster stores in its instance
   (`$DAGSTER_HOME/storage/...`).

The data_version is **how Dagster decides downstream staleness**.
You'll feel that in `lab2-deps`.

## Common pitfalls

- **`dagster dev` finds nothing**: you ran from the wrong dir, or the
  `-m hello` doesn't match the package name. `cd` to the lab dir
  first.
- **"Reload" button is greyed out**: dagster dev is in a partial
  failure state. Check the terminal for traceback.
- **Materialize button doesn't do anything**: a popup may have been
  blocked. Look at the bottom-right of the Dagster UI for queued
  runs.

## Cheat sheet

```python
from dagster import asset, MaterializeResult, DataVersion, Definitions

@asset
def my_asset() -> MaterializeResult:
    return MaterializeResult(
        data_version=DataVersion("v1"),
        metadata={"size_bytes": 42},
    )

defs = Definitions(assets=[my_asset])
```

That's the smallest valid Dagster file. Everything in later labs is a
generalization.
