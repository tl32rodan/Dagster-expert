# 1_13_10_RELEASE_NOTES.md — what changed through 1.13.10 that matters here

> **Source**: `github.com/dagster-io/dagster/blob/1.13.10/CHANGES.md`,
> entries for **1.13.8, 1.13.9, 1.13.10** (the delta on top of the prior
> 1.13.7 corpus). Filtered for items that could affect the Execution
> Fabric framework or the air-gap deploy. Everything under
> `dg` / UI / Dagster+ / k8s / dbt / databricks / ClickHouse is
> out-of-scope for this deploy and omitted.
>
> **Provenance / validation status**: this corpus was curated from the
> upstream CHANGES.md, NOT from training memory. The framework's 49-test
> suite + end-to-end demo were last run green against an **installed
> 1.13.7** (this workstation's current pinned build). 1.13.8–1.13.10 are
> bugfix/minor-feature releases with **no breaking API change** to
> anything the framework uses (see "confirmed unchanged" below), so those
> results carry forward. Re-run `pytest` + `scripts/run_demo.py` +
> `dagster definitions validate` once the 1.13.10 build is actually
> deployed to reconfirm.

## TL;DR — what's load-bearing (1.13.8 → 1.13.10)

Nothing changes the framework's public-API contract. Four bugfixes are
worth knowing; the rest is out-of-scope.

1. **[1.13.9] Event-log-watcher deadlock on shutdown (Postgres/MySQL) is
   fixed.** Most relevant item: production run/event storage is
   PostgreSQL, and this fixes a deadlock that could occur when shutting
   down event log watchers. Improves clean daemon shutdown/restart — the
   restart-safety story the harvest sensor relies on (`SENSORS.md` §2).
2. **[1.13.9] YAML no longer coerces date-like strings to `datetime`.**
   Relevant to `spec.yaml` parsing. Previously a bare value that *looked*
   like a date/time in a YAML config could be silently loaded as a
   `datetime` object instead of a string. If a flow ever uses a
   date-like dimension value or config token, this fix means it stays a
   string. (The Pydantic loader in `framework/spec/loader.py` is the
   consumer.)
3. **[1.13.8] `InstigationLogger` stringifies non-JSON-serializable log
   record attributes.** Relevant to sensor logging: the dispatch and
   harvest sensors log via `context.log` (an `InstigationLogger`).
   Pre-1.13.8, logging a record carrying a non-serializable attribute
   could error; now it is stringified. Makes sensor error logging
   (e.g. harvest's per-row failure log) more robust.
4. **[1.13.10] `get_latest_materialization_event` stale-after-wipe fix —
   TANGENTIAL, does not change Fabric behavior.** This fixes stale
   returns from `DagsterInstance.get_latest_materialization_event` for
   wiped assets. The Execution Fabric reads its `observed` set from the
   **status DB**, not from this API (WHITEPAPER §3.3 / R7), so this does
   not affect dispatch/harvest correctness. Noted only so a reader
   doesn't assume it does.

## Security fixes in range (N/A here, listed for completeness)

- **[1.13.8] SQL-injection fix in the ClickHouse libraries** when using
  dynamic partition keys. **Not applicable** — this deploy uses no
  ClickHouse. Listed so a security audit sees it was considered.

## Items confirmed UNCHANGED 1.13.7 → 1.13.10

The corpus's core API claims still hold. None of the following changed
signature or semantics in 1.13.8/.9/.10 (verified against CHANGES.md):

- `DagsterInstance.report_runless_asset_event(AssetMaterialization(...))`
  — the harvest sensor's core contract (append-only, latest-wins).
- `@dg.sensor(job=..., default_status=..., minimum_interval_seconds=...)`
  — including the `job=` schema requirement for side-effect-only sensors.
- `SensorEvaluationContext.cursor` (plain string; JSON-encode yourself)
  and `SensorResult(run_requests=..., cursor=..., skip_reason=...)`.
- `RunLauncher` ABC (`launch_run`, `terminate`,
  `check_run_worker_health`, `supports_check_run_worker_health`) and
  `DefaultRunLauncher`.
- `QueuedRunCoordinator` config keys (`max_concurrent_runs`,
  `tag_concurrency_limits`, `dequeue_use_threads`, `dequeue_num_workers`).
- `MultiToSingleDimensionPartitionMapping` — **still beta** (still emits
  `BetaWarning`); no changelog item promoted it to stable.
- `@dg.asset` body returning `None` still auto-emits a placeholder
  `ASSET_MATERIALIZATION` with an auto-computed `data_version` — the
  foundation of the dispatch-asset-body pattern.
- `define_asset_job` still rejects mixed partition shapes in one
  selection (one job per asset / per shape).
- `instance.get_materialized_partitions`,
  `get_latest_data_version_record`, `logs_after`, `get_runs`,
  `get_records_for_run`.

## Air-gap caveats unchanged

All `AIRGAP_DELTAS.md` items still apply. 1.13.8–1.13.10 did not change
`telemetry: { enabled: false }`, the ability to run without `dg`/`uv`,
or any PostgreSQL storage behavior relevant to this deploy.

## If you're looking for…

| Looking for | Status through 1.13.10 |
|---|---|
| Components / `dagster_components` | Still `dg`-only; not used here |
| `AutomationCondition.eager()` | Exists; we don't use it (0-eval per tick at multi-partition cross-product scale; Fabric uses its own sensors) |
| `AssetCheck` | Exists; not used by the Fabric framework |
| Pipes (`PipesSubprocessClient`) | Exists; not used by the Fabric framework (worker writes status DB directly) |
| `report_runless_asset_event` for FAILED events | Yes — `AssetFailedToMaterialize` is the dual; the Fabric emits SUCCESS-only and leaves FAILED rows un-redispatched until upstream data_version changes |

## Upgrade note (1.13.7 → 1.13.10)

Patch bump within the same minor: **no code changes required** in the
framework or flows. The bump path is:
1. Deploy the 1.13.10 build into the air-gap venv (wheelhouse:
   `pip install --no-index --find-links=~/wheelhouse dagster==1.13.10`).
2. Re-run `pytest`, `scripts/run_demo.py`, and
   `dagster definitions validate -m examples.<NN>_<topic>` to reconfirm
   green on the actual 1.13.10 build.
3. The ROLE / SKILL pre-flight gates now expect `dagster --version` to
   report **1.13.10**; a 1.13.7 venv will (correctly) be refused.
