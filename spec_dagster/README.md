# spec_dagster — five-layer spec-driven Dagster framework (D1 reference build)

> Built 2026-06-04 as the **D1** deliverable: a working slice of
> `personalities/flow-cartographer/FIVE_LAYER_WHITEPAPER.md`, with
> `liberate-char` expressed as a spec + script and driven end-to-end
> by **daemon + sensor** (local-sim mode: DefaultRunLauncher + SQLite
> + mock bsub).
>
> **Why at repo top-level (not under `personalities/flow-cartographer/`)**:
> this is framework *code*, not personality content. It has its own
> pytest suite, demo harness, equivalence harness, and `ONBOARDING.md`;
> it is meant to be independently runnable and packageable. The
> personality at `personalities/flow-cartographer/` owns the
> architecture spec (whitepaper) + the conversion plan + the
> hand-rolled equivalence reference under `examples/`.
>
> This is the first real implementation of the whitepaper. Lessons
> learned during the build are captured in `LESSONS.md` (and folded
> back into the whitepaper's implementation contracts).

## Layout

```
spec_dagster/
  framework/                 # the framework (import as `framework.*`)
    spec/         schema.py + loader.py        # M1 — Pydantic spec + validation
    assets/       mapping_builder.py           # M2 — intent → PartitionMapping
                  partition_builder.py         # M2 — dimensions → PartitionsDefinition
                  builder.py                   # M2 — script → @asset
    sensor/       reconcile.py + planner.py    # M3 — desired−observed reconciliation
                  factory.py                   # M3 — build_sensor
    versioning/   base.py                      # data version (content_hash / timestamp / custom)
    generator.py                               # M2 — build_definitions(flows_dir)
  flows/
    liberate_char/  spec.yaml + script.py + _vendor/  # the application (flow owner)
  tests/                                       # pure-function pytest (TDD)
```

## Run (local-sim, daemon + sensor)

```bash
# from this directory, with the dagster venv:
setenv PYTHONPATH $PWD                 # tcsh  (bash: export PYTHONPATH=$PWD)
setenv DAGSTER_HOME $PWD/.dagster_home
setenv PATH $PWD/flows/liberate_char/_vendor/bin:$PATH   # mock bsub on PATH
python -m scripts.run_demo             # bootstrap generators + start daemon + verify
```

See `LESSONS.md` for what the build taught us about the whitepaper.

## Scope (what this D1 slice does / does not cover)

- **Does**: M1 spec + validation; M2 generator (mappings, partitions,
  assets, Definitions); M3 reconciliation sensor; local-sim execution
  via DefaultRunLauncher + QueuedRunCoordinator + daemon; the full
  liberate-char graph (6 generators + 9 characterize leaves) verified
  end-to-end against the hand-rolled `converted/` outputs.
- **Does not** (whitepaper sections deferred): the M4 custom
  `LSFRunLauncher` (local-sim uses DefaultRunLauncher); Postgres backend
  (local-sim uses SQLite); the `work_items` dimensionality-reduction +
  `plan_batches` batching sensor (liberate-char partitions cell directly
  rather than reducing it to config — that path is the *other* reference
  flow, `netlist_files`).
```
