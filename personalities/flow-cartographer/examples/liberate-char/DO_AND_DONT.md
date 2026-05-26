# DO & DON'T — converting a flow to Dagster 1.13.3 (operating rules)

Rules the conversion agent **must** follow. They sit on top of
`personalities/dagster-expert/database/dagster-1.13.3/docs/STANDARD_USAGE.md`
(cite it, don't duplicate it). Each rule is mechanical and maps to a
`flow-cartographer` verify-loop check. The `liberate-char` example in this
folder is the worked reference — **mimic its structure**, don't reinvent.

> Why this exists: a weak agent left to "design" tends to improvise — it
> shortcuts to reading the original paths, picks the wrong partition mapping,
> uses a deprecated/imagined API, skips tests, and you only find out at the
> end. These rules remove the decisions.

## The workflow (do it in this order)

1. **Extract, don't copy.** Put every hardcoded / duplicated source value into
   `config/*.yaml`. Write a generator that renders each source file from config.
2. **Test the generator FIRST.** `tests/` must prove the generator reproduces
   the original files byte-for-byte (path-free files exactly; path-bearing files
   identical given the same root). Stdlib `unittest` — runs air-gapped.
3. **Wire assets** from the generated sources (folder-as-asset).
4. **Prove equivalence.** `_smoke.py` materializes everything and asserts the
   products differ from the originals **only in embedded paths** (`diff_proof`).
5. **One increment at a time**, cite `$FLOW_SRC/<file>:<line>` + the Dagster API,
   park ambiguities in `_open_questions.yaml`. Never change approach mid-flight.

## DO / DON'T

| DO (verified on the install) | DON'T |
|---|---|
| Generate every source from `config/liberate.yaml` + `pipelines/generators.py`. | ❌ Copy a flow-src file in unchanged, or read the original `$FLOW_SRC` path at runtime. |
| Prove correctness with `diff_proof` / `_smoke.py` (only paths differ). | ❌ Eyeball "looks right" / mark done with caveats. |
| Write `unittest` for core + generator-equivalence **before** assets. | ❌ Wire assets first, test later (or never). |
| One **module-level singleton** `partitions_def` per shape (`spec/partitions.py`), imported everywhere. | ❌ Construct a fresh `partitions_def` per asset (mapping silently degrades to "all partitions"). |
| 2-axis `MultiPartitionsDefinition({"pvt":…, "cell":…})`. Key string is **`cell\|pvt`** (dims sorted alphabetically). | ❌ Assume >2 dimensions, or assume the key order matches definition order. |
| Cross-dimension dep = `AssetDep(key, partition_mapping=MultiToSingleDimensionPartitionMapping(partition_dimension_name="pvt"\|"cell"))`, memoized; attach with `deps=`. | ❌ Subclass `PartitionMapping` (breaks reconciliation), or use `ins=` (forces IO load). |
| Auto-rebuild via **`automation_condition=AutomationCondition.eager()`** (the current 1.13.3 API). | ❌ `auto_materialize_policy=AutoMaterializePolicy.eager()` — **deprecated** in this 1.13.3 (`@asset` has no such param). |
| Cluster/parallel work = asset-body `bsub` via `PipesSubprocessClient` + `QueuedRunCoordinator` (`max_concurrent_runs` + `tag_concurrency_limits`). | ❌ A custom `RunLauncher` / "multi-thread run launcher" (wrong layer; can't parallelize a backfill — STANDARD_USAGE §9c). |
| Trigger via **sensor** (golden path) + targeted CLI `--select … --partition`. | ❌ UI "Materialize all" / routine wide `backfill`. |
| Public API only; every symbol exists in `database/dagster-1.13.3/docs/`. | ❌ `dagster._core/_internal/_private` imports. |

## The meta-rule: VERIFY against the installed library

Before writing any Dagster symbol, confirm it on the actual install:

```bash
python -c "import inspect,dagster; from dagster import X; print(inspect.signature(X))"
```

The corpus itself can be stale. Building this example we found
`STANDARD_USAGE.md §5/§7.3` prescribes `AutoMaterializePolicy.eager()`, but on
the real `dagster==1.13.3` that is **deprecated** and `@asset` takes
`automation_condition=AutomationCondition.eager()` instead. STANDARD_USAGE §5
already says *"verify exact API on your install"* — so do it. Trusting memory or
a doc over the install is the #1 source of confidently-wrong code.

## Maps to verify-loop checks

| Rule | verify-loop check |
|---|---|
| API exists on install / in corpus | 1 `invented-api` |
| no `_core/_internal/_private` | 2 `private-import` |
| `_smoke.py` runs, exit 0 | 3 `smoke-failed` |
| generated, not copied (generator reproduces sources) | 4 `not-converted` |
| cite `$FLOW_SRC:line` + API | 5 `uncited` |
| state / rerun / scheduling / deps / logs preserved | 6 `coverage-gap` |
