# Phase 1 Implementation Plan — `liberate-char` on Execution Fabric

> **Status**: PLAN ONLY. No code changes yet. This file is the execution
> companion to `EXECUTION_FABRIC_WHITEPAPER.md` (v2), targeted at the
> v2-Phase-1 scope (`whitepaper §8`).
>
> **Starting point**: the v1 implementation in `spec_dagster/` (D1+D2,
> PR #18) is the assumed baseline. Phase 1 *evolves it* by replacing
> the execution layer; the spec / mapping / partition / versioning
> layers are reused largely as-is. **It does not delete v1**;
> see decisions §1 below.

## 0. What changed (the architectural pivot)

| Concern | v1 (`FIVE_LAYER_WHITEPAPER.md`, `spec_dagster/`) | v2 Phase 1 (this plan) |
|---|---|---|
| Dagster's role | lineage **+** execution (custom `LSFRunLauncher` ships whole runs to LSF) | lineage **only**; execution outsourced |
| `LSF run worker` lifetime | = entire computation duration | seconds (asset body dispatches, returns immediately; harvest sensor backfills) |
| Run launcher | `LSFRunLauncher` (1 run = 1 bsub) | `DefaultRunLauncher` (whitepaper §3.1) |
| Execution truth | Dagster event log | **Status DB** (SQLite + write file lock) |
| LSF dispatch unit | the run worker process | a single computation script (not a Dagster run) |
| sensor model | reconcile (`desired − observed` → `RunRequest`) | **two** sensors: dispatch (`desired − observed` → status-DB INSERT + bsub) + harvest (status-DB SUCCESS → `MaterializeResult`) |
| daemon bottleneck risk | yes (queue dispatch is push-mode; coordinator launch throughput) | no (asset body is short; LSF run client fires-and-forgets) |

The motivating regime (whitepaper §1.1): **10k+ concurrent long
computations with ~1% incremental re-run**. v1 chokes the orchestrator;
v2 keeps the orchestrator small and pushes load to LSF.

## 1. Decisions (recommended; user may override before execution)

| # | Decision | Recommended | Why |
|---|---|---|---|
| 1 | **Migration mode**: in-place rewrite of `spec_dagster/`, or parallel new directory | **parallel** new `execution_fabric/` at repo top-level | (a) PR #18 stays a clean v1 reference; (b) v2 is an architecture inversion, not a feature add — diffing in-place will produce a noisy/misleading patch; (c) we can run both verifications in parallel for a clean handoff |
| 2 | **v1 whitepaper handling** | KEEP `FIVE_LAYER_WHITEPAPER.md` + ADD a "superseded by EXECUTION_FABRIC_WHITEPAPER.md" pointer at its top | The v1 doc has the "why we tried this and it didn't scale" record; that's reference for future agents not to repeat the experiment |
| 3 | **v1 framework code (`spec_dagster/`) handling** | Freeze as v1 reference; do not delete; do not edit. Mark in its README as "v1, superseded by `execution_fabric/`" once v2 lands | Deleting hides the lineage of decisions; the equivalence harness + LSF launcher tests stay as evidence |
| 4 | **Top-level dir name for v2** | `execution_fabric/` | Matches whitepaper §3.2 terminology; not "spec_dagster_v2" because the spec is no longer the most distinctive layer (execution fabric is) |
| 5 | **Phase 1 status DB layout** | One SQLite file per flow (`execution_fabric/state/<flow_name>.db`) | Per-flow isolation simplifies reasoning + concurrent flows can't cross-corrupt; Phase 2's PostgreSQL can flatten this if needed |
| 6 | **Phase 1 equivalence harness** | Build a **new** `execution_fabric/scripts/run_demo.py` (v2 demo) + a **new** `scripts/equivalence_v2.py` (vs v1 reference, not vs `examples/converted/`) | The v1 vs hand-rolled axis was D2's question; here the question is "does v2 produce the same .ldb digests + same Dagster lineage as v1?" |
| 7 | **Branching for v2 work** | Open a **new branch** after PR #18 merges (or in parallel with it as a draft); do NOT mix v2 commits into PR #18 | PR #18's scope is v1 D1+D2; mixing v2 dilutes both reviews |

(Open questions that affect implementation but don't block planning are
listed at the bottom of this file, §10.)

## 2. Architecture layer map (v1 → v2 Phase 1)

```
v1 (spec_dagster/)                    v2 Phase 1 (execution_fabric/)
─────────────────────                ─────────────────────────────────────
M1 Spec (schema + loader)    ──────→  Spec (verbatim reuse)
M2 Generator (build_definitions)  ──→  Generator (rewritten: dispatch+harvest sensors)
   ├─ partition_builder.py   ──────→  partition_builder.py (verbatim)
   ├─ mapping_builder.py     ──────→  mapping_builder.py (verbatim)
   └─ builder.py             ──────→  builder.py (rewritten: dispatch semantics)
M3 reconcile sensor          ──────→  SPLIT into:
                                       ├─ dispatch sensor (desired−observed → status DB INSERT + bsub)
                                       └─ harvest sensor  (status DB SUCCESS → MaterializeResult)
M4 LSFRunLauncher            ──────→  REMOVED (use DefaultRunLauncher per §3.1)
M5 PipesSubprocessClient     ──────→  REMOVED (no in-asset subprocess)
   (in-asset execution)              Replaced by:
                                       ├─ LSF run client (lsf_run_client.py)
                                       └─ Fabric worker script (runs ON LSF node)
Versioning (content_hash)    ──────→  Versioning (verbatim)
                              NEW →   Execution Fabric:
                                       ├─ status_db.py (SQLite + schema)
                                       ├─ file_lock.py (write lock helper)
                                       └─ (Phase 2: lsf_synchronizer.py, reaper.py, dispatcher_queue.py)
```

## 3. Module-level breakdown

```
execution_fabric/                              # NEW top-level dir (parallel to spec_dagster/)
├── README.md                                  # NEW: what this is + relationship to spec_dagster/
├── ONBOARDING.md                              # NEW: flow-owner SOP for v2
├── LESSONS.md                                 # NEW: capture v2 build lessons (same shape as v1's)
├── PHASE1_LIMITATIONS.md                      # NEW: copy of whitepaper §8.3 with concrete repro recipes
├── .gitignore                                 # NEW: ignore .dagster_home_*/ .eq_*.json state/*.db
├── framework/
│   ├── __init__.py
│   ├── spec/
│   │   ├── __init__.py                        # COPY from spec_dagster
│   │   ├── schema.py                          # COPY verbatim
│   │   └── loader.py                          # COPY verbatim (incl. _* dir skip)
│   ├── assets/
│   │   ├── __init__.py                        # COPY
│   │   ├── partition_builder.py               # COPY verbatim
│   │   ├── mapping_builder.py                 # COPY verbatim
│   │   └── builder.py                         # REWRITE: see §3.1 below
│   ├── versioning/
│   │   ├── __init__.py                        # COPY
│   │   └── base.py                            # COPY verbatim
│   ├── sensor/
│   │   ├── __init__.py
│   │   ├── dispatch.py                        # NEW: see §3.2
│   │   ├── harvest.py                         # NEW: see §3.3 (the fragile one — TDD priority)
│   │   └── planner.py                         # COPY verbatim (still useful for dispatch sensor)
│   ├── fabric/                                # NEW execution layer
│   │   ├── __init__.py
│   │   ├── status_db.py                       # NEW: see §3.4
│   │   ├── file_lock.py                       # NEW: see §3.5
│   │   ├── lsf_run_client.py                  # NEW: see §3.6
│   │   └── schema.sql                         # NEW: status DB DDL (Phase-2-ready columns)
│   ├── generator.py                           # REWRITE: build dispatch_sensor + harvest_sensor; use DefaultRunLauncher
│   └── config/
│       └── dagster.fabric.yaml                # NEW: Phase 1 instance config
├── flows/
│   ├── __init__.py
│   ├── _template/                             # NEW (mirror v1, adjust dispatch semantics)
│   └── liberate_char/
│       ├── __init__.py
│       ├── spec.yaml                          # COPY from spec_dagster (small tweak: dispatch: fabric)
│       ├── script.py                          # ADAPT: compute fn returns a "fabric task spec" not argv
│       ├── _vendor/                           # COPY verbatim (mock liberate / bsub / config / generators)
│       ├── fabric_worker.py                   # NEW: runs ON LSF node; see §3.7
│       ├── definitions.py                     # COPY (one-line build_definitions call)
│       └── workspace.yaml                     # COPY
├── tests/
│   ├── test_spec_schema.py                    # COPY verbatim
│   ├── test_mapping_builder.py                # COPY verbatim
│   ├── test_partition_builder.py              # COPY verbatim
│   ├── test_versioning.py                     # COPY verbatim
│   ├── test_planner.py                        # COPY verbatim
│   ├── test_status_db.py                      # NEW: schema + state-machine transitions + delete-after-harvest
│   ├── test_file_lock.py                      # NEW: concurrent-write safety
│   ├── test_lsf_run_client.py                 # NEW: mock bsub + script-side data-version + status-DB write
│   ├── test_dispatch_sensor.py                # NEW: desired−observed → INSERT + bsub (idempotency-aware)
│   ├── test_harvest_sensor.py                 # NEW (TDD priority): cursor + idempotent backfill + restart
│   └── _mock_lsf/{bsub,bjobs,bkill}           # COPY from spec_dagster/tests/_mock_lsf
└── scripts/
    ├── __init__.py
    ├── run_demo.py                            # NEW: end-to-end Phase 1 demo
    └── equivalence_v2.py                      # NEW: compare v2 vs v1 reference (.ldb digests must match)
```

### 3.1 `framework/assets/builder.py` — REWRITE

**v1 behavior** (current): compute asset body uses `PipesSubprocessClient.run(argv)`
in-process, blocks until subprocess finishes, returns `MaterializeResult`
with data_version from Pipes.

**v2 Phase 1 behavior**:
- Compute asset body:
  1. Compute the **idempotency key** = `(asset_name, partition_key, trigger_fingerprint)`
     where `trigger_fingerprint = sha256(upstream_data_versions sorted)` (whitepaper §6 prelude).
  2. Call `fabric.lsf_run_client.dispatch(idempotency_key, argv)` which
     does `bsub` and **does not block**.
  3. Return immediately. **Key open question** (§10 below): does the
     asset body `yield MaterializeResult` here (without data_version) or
     skip materialization entirely (let harvest sensor be the sole
     event source)? — Plan recommends OPTION A: skip
     materialization; harvest sensor is the only producer of
     materialization events. Probe 1.13.3 before final commit.
- Generator asset body: **unchanged** (it's lightweight, runs in-process,
  produces files + content_hash data_version).
- Entry asset body: unchanged.

Concretely:
```python
# v2 compute asset body (sketch):
@dg.asset(...)
def _compute(context):
    vals = _partition_values(context, asset_spec.partitioned_by)
    argv = script_fn(*vals)  # same as v1: script returns argv
    upstream_dvs = _collect_upstream_data_versions(context, asset_spec)  # whitepaper §6
    idem_key = sha256(f"{asset_spec.name}|{context.partition_key}|"
                       f"{'|'.join(sorted(upstream_dvs))}").hexdigest()
    fabric.dispatch(
        idempotency_key=idem_key,
        asset_name=asset_spec.name,
        partition_key=context.partition_key,
        argv=argv,
        upstream_data_versions=upstream_dvs,
    )
    # No MaterializeResult here. Harvest sensor will produce it.
    # (See §10 Q4: confirm 1.13.3 allows an asset body to exit without
    # MaterializeResult; if not, fallback to yielding a placeholder.)
```

### 3.2 `framework/sensor/dispatch.py` — NEW

Replaces v1's reconcile-and-RunRequest sensor. Semantics:
- Read `desired = compute_asset.partitions_def.get_partition_keys()`.
- Read `observed = status_db.list_keys_in_terminal_state(asset_name)`
  (NOT `instance.get_materialized_partitions` — that's harvest's
  truth, see §3.3).
- For `missing = desired − observed`:
  - Compute idempotency_key per partition.
  - `status_db.upsert_pending(idem_key, ...)` (Phase 1: simple insert; Phase 2: dedup against active states per whitepaper §6.1).
  - Either (a) emit a Dagster `RunRequest` that fires the asset body
    (which itself dispatches), OR (b) call `lsf_run_client.dispatch`
    directly from the sensor. Plan recommends (a) — keeps Dagster's
    run accounting + sensor stays cheap.
- `default_status=DefaultSensorStatus.RUNNING` (v1 lesson L6 still applies).

### 3.3 `framework/sensor/harvest.py` — NEW (TDD priority)

The single most fragile piece (whitepaper §6.3). Semantics:
- Cursor: `cursor = json.dumps({"last_storage_id": int})` of the
  highest status-DB row processed.
- Per tick:
  1. `rows = status_db.list_unharvested_terminals(after_id=cursor)`
     (sorted by id ascending; LIMIT batch to keep tick bounded).
  2. For each row:
     - Idempotently emit a materialization event. **Option A** (preferred):
       `context.instance.report_runless_asset_event(...)` if 1.13.3
       supports it. **Option B**: emit `AssetMaterialization` via
       `context.log_event(...)` from within the sensor's tick context.
       **Probe before commit** (see §10 Q4).
     - Mark the row `harvested=true` in status DB (or delete it —
       whitepaper §8.4 mandates delete).
  3. Advance cursor AFTER all backfills complete.
  4. Whitepaper §6.3 invariant: cursor falls back, never overshoots.

Tests (test_harvest_sensor.py) MUST cover:
- repeat tick → no duplicate materializations
- sensor restart mid-batch → resumed cleanly
- partial backfill failure → cursor stays put, retried next tick
- delete-after-harvest → status DB doesn't grow unboundedly

### 3.4 `framework/fabric/status_db.py` — NEW

SQLite schema (Phase-2-ready, even though Phase 2 fields unused):
```sql
-- schema.sql (Phase 1)
CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL,            -- (asset, partition, fingerprint) sha256
    asset_name      TEXT NOT NULL,
    partition_key   TEXT,                     -- nullable for unpartitioned
    state           TEXT NOT NULL,            -- PENDING / SUBMITTED / RUNNING / SUCCESS / FAILED
    lsf_job_id      TEXT,                     -- set on bsub (Phase 1 may leave NULL)
    worker_id       TEXT,                     -- set when claimed (Phase 2)
    lease_expires   INTEGER,                  -- epoch seconds (Phase 2 reaper)
    data_version    TEXT,                     -- set by worker on SUCCESS
    error_message   TEXT,
    submitted_at    INTEGER,
    running_at      INTEGER,
    terminal_at     INTEGER,
    harvested       INTEGER NOT NULL DEFAULT 0,  -- harvest sensor flips to 1 (or row deleted)
    UNIQUE (idempotency_key)                  -- enforce whitepaper §6.1 invariant at DB level
);
CREATE INDEX IF NOT EXISTS ix_tasks_state_id  ON tasks(state, id);
CREATE INDEX IF NOT EXISTS ix_tasks_harvested ON tasks(harvested);
```

Public API (small, opinionated):
- `init_db(path)` — create file + apply schema; idempotent
- `upsert_pending(idem_key, asset, partition, ...)` → returns existing
  row if same key already active (whitepaper §6.1 contract; Phase 1 may
  rely solely on UNIQUE constraint)
- `mark_submitted(idem_key, lsf_job_id)`
- `mark_running(idem_key)` — used by Phase 2 LSF synchronizer
- `mark_success(idem_key, data_version)` — called by fabric_worker.py on LSF node
- `mark_failed(idem_key, error_message)`
- `list_unharvested_terminals(after_id, limit)` → harvest sensor input
- `mark_harvested(id_list)` OR `delete_harvested(id_list)` — whitepaper §8.4

ALL writes go through `file_lock.with_write_lock(path)` (see §3.5).

### 3.5 `framework/fabric/file_lock.py` — NEW

Whitepaper §8.4 hard requirement. Use `fcntl.flock(fd, LOCK_EX)` on
a sibling lockfile (`<db>.lock`) so SQLite's own busy-handler isn't
the sole arbiter. Pattern:
```python
@contextmanager
def with_write_lock(db_path: Path):
    lock_path = db_path.with_suffix(db_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
```
Tests cover: two processes writing simultaneously serialize correctly;
deadlock recovery (lockfile orphaned by killed process → next flock
succeeds because file system tracks lock ownership by FD).

### 3.6 `framework/fabric/lsf_run_client.py` — NEW

Replaces v1's `LSFRunLauncher.launch_run` + the in-asset
`PipesSubprocessClient.run`. Semantics (whitepaper §8.2):

```python
def dispatch(idempotency_key, asset_name, partition_key,
             argv, upstream_data_versions, db_path,
             lsf_queue, lsf_cores, lsf_mem_mb, lsf_walltime,
             ...):
    # 1. INSERT a PENDING row (idempotent — UNIQUE constraint absorbs dupes).
    with status_db.file_lock.with_write_lock(db_path):
        status_db.upsert_pending(idem_key=idempotency_key, ...)
    # 2. Build the bsub command around fabric_worker.py
    bsub_argv = [
        "bsub", "-K" if False else "",   # NOT -K: we don't block!
        "-J", f"fabric_{idempotency_key[:8]}",
        "-q", lsf_queue, "-n", str(lsf_cores),
        "-R", f"rusage[mem={lsf_mem_mb}]", "-W", lsf_walltime,
        "-o", f"{LOG_DIR}/{idempotency_key}.out",
        "-e", f"{LOG_DIR}/{idempotency_key}.err",
        "-env", "DAGSTER_HOME,PATH,PYTHONPATH",
        # The fabric_worker.py is what the LSF node executes:
        sys.executable, str(FABRIC_WORKER),
        "--db-path", str(db_path),
        "--idempotency-key", idempotency_key,
        "--asset-name", asset_name,
        "--partition-key", partition_key or "",
        "--", *argv,   # the actual computation argv from script.py
    ]
    # 3. Fire-and-forget bsub. Parse job id, mark SUBMITTED.
    proc = subprocess.run(bsub_argv, capture_output=True, text=True, check=True)
    job_id = parse_bsub_output(proc.stderr + proc.stdout)
    with status_db.file_lock.with_write_lock(db_path):
        status_db.mark_submitted(idempotency_key, lsf_job_id=job_id)
```

Critical: **no `-K`**, no waiting. The asset body returns long before
the LSF job runs.

### 3.7 `flows/liberate_char/fabric_worker.py` — NEW

Runs ON the LSF node. Replaces the in-asset Pipes loop. Semantics
(whitepaper §8.2 steps 2–4):

```python
# CLI invoked by lsf_run_client's bsub:
#   fabric_worker.py --db-path ... --idempotency-key ... --asset ... \
#                    --partition ... -- <script argv>
def main():
    args, computation_argv = parse_args()
    try:
        # set -e; set -o pipefail equivalent:
        # any subprocess failure is fatal, no half-written state
        subprocess.run(computation_argv, check=True)
        # data_version is computed from script outputs (e.g. .ldb digest)
        data_version = _compute_data_version_from_outputs(args)
        with status_db.file_lock.with_write_lock(args.db_path):
            status_db.mark_success(args.idempotency_key, data_version)
    except Exception as e:
        with status_db.file_lock.with_write_lock(args.db_path):
            status_db.mark_failed(args.idempotency_key, str(e)[:1000])
        raise
```

For liberate-char specifically, `_compute_data_version_from_outputs`
reads the `.ldb` file's `digest` line (same as v1's `liberate_inner.py`).

### 3.8 `framework/config/dagster.fabric.yaml` — NEW

Per whitepaper §3.1:
```yaml
run_launcher:
  module: dagster._core.launcher
  class: DefaultRunLauncher          # NOT a custom launcher

run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    # Tiny — these runs are dispatch-only or harvest-only, both short.
    max_concurrent_runs: 8

run_monitoring:
  enabled: true

telemetry:
  enabled: false

# Storage: leave default SQLite at $DAGSTER_HOME for Phase 1.
# Phase 2 switches to PostgreSQL per whitepaper §9.1.

# Note: Dagster UI deliberately NOT used (whitepaper §3.3); we ship
# instance config but operators only run `dagster-daemon`, no webserver.
```

## 4. Test plan

Order corresponds to TDD-priority dependency:
1. `test_status_db.py` (pure-ish: schema migrations, state transitions, UNIQUE enforcement)
2. `test_file_lock.py` (concurrent-write determinism)
3. `test_lsf_run_client.py` (mock bsub on PATH: argv assembly, status-DB INSERT under lock, job-id parse)
4. `test_harvest_sensor.py` ← **highest priority**: cursor + idempotency + restart + partial-failure (whitepaper §6.3)
5. `test_dispatch_sensor.py` (desired−observed → upsert + RunRequest)
6. `scripts/run_demo.py`: end-to-end with mock bsub on PATH — daemon + dispatch sensor + LSF run client + fabric_worker + harvest sensor → 9/9 characterize partitions visible in Dagster (via harvest), 9 SUCCESS rows in status DB (then harvested), 9 `.ldb` files
7. `scripts/equivalence_v2.py`: run v2 demo; collect `dagster/data_version` per partition; assert == v1 spec_dagster demo's values; assert `.ldb` digests byte-equal v1's

## 5. Phase 1 limitations (per whitepaper §8.3) — explicit acceptance

These ARE accepted (i.e. tested for known failure modes but NOT fixed):
| # | Limitation | Phase 1 behavior | Phase 2 plan |
|---|---|---|---|
| L1 | SQLite single-writer | file lock serializes writes; OK for verification, NOT for 10k production | PostgreSQL |
| L2 | No orphan recovery | If fabric_worker.py crashes between `bsub` and `mark_success`, the row stays SUBMITTED forever | reaper + bjobs synchronizer |
| L3 | No dispatch dedup beyond UNIQUE constraint | Repeat triggers → UNIQUE INSERT swallows silently; but no smart conflict resolution | full §6.1 contract |
| L4 | No SUBMITTED/RUNNING distinction by bjobs | Phase 1: SUBMITTED is just "bsub returned"; we never poll bjobs | LSF synchronizer (whitepaper §5) |
| L5 | Sensor not idempotent across data_version changes for the same partition | Phase 1: triggers on missing only; if upstream data_version changes, dispatch sensor re-dispatches (different idem_key) but old row remains in status DB taking space | Phase 2 reaper + smart cleanup |

These are NOT acceptable to drop in Phase 1 (whitepaper §8.4):
- ✅ dispatch/harvest separation (asset body must not block)
- ✅ write file lock (correctness floor)
- ✅ harvest sensor's delete-after-harvest (or `harvested=1` flag → cleanup loop)
- ✅ idempotency key schema column (Phase-2 readiness)

## 6. Effort estimate

| Phase | Effort | Notes |
|---|---|---|
| Scaffold + copy reusable modules | 0.5 d | mostly `git cp` |
| `status_db.py` + schema.sql + tests | 1.0 d | |
| `file_lock.py` + tests | 0.5 d | |
| `lsf_run_client.py` + tests (mock bsub) | 1.0 d | |
| `fabric_worker.py` + liberate-char adapter | 0.5 d | |
| Asset builder rewrite (dispatch semantics) | 1.0 d | |
| Dispatch sensor + tests | 0.5 d | |
| **Harvest sensor + tests (TDD priority)** | **1.5 d** | most fragile; over-test |
| `dagster.fabric.yaml` + bootstrap glue | 0.5 d | |
| `scripts/run_demo.py` (end-to-end) | 1.0 d | |
| `scripts/equivalence_v2.py` + run | 0.5 d | |
| Docs (README, ONBOARDING, LESSONS, PHASE1_LIMITATIONS) | 0.5 d | |
| **Total** | **≈ 8.5 d** | assumes v1 codebase as baseline |

## 7. Reuse audit (what we are NOT building)

Verbatim copies from `spec_dagster/`:
- `framework/spec/{schema,loader}.py` — already validated by 25 pytest
- `framework/assets/{mapping,partition}_builder.py` — already validated
- `framework/versioning/base.py` — already validated
- `flows/liberate_char/_vendor/*` — mock liberate/bsub/render/config; the deterministic-digest contract holds
- `flows/liberate_char/spec.yaml` — same flow, same dependencies
- `flows/liberate_char/script.py` generator functions — pure functions, kind agnostic
- `tests/_mock_lsf/{bsub,bjobs,bkill}` — mock LSF shims

This reuse is the payoff of v1 + D2: the *lineage* layer (whitepaper §3.1)
was already separated and tested, so v2 keeps it whole and only swaps
out the *execution* layer.

## 8. Migration / cutover

1. Land v2 code in `execution_fabric/` (parallel; v1 untouched).
2. Reach behavioral parity: `scripts/equivalence_v2.py` shows v2 produces
   the same data_versions + .ldb digests as v1's spec_dagster demo.
3. Mark v1 as v1 reference in `spec_dagster/README.md` (one paragraph
   at top).
4. Update `personalities/flow-cartographer/README.md` (the INDEX from
   PR #18) to surface v2 as the active path.
5. The actual production cutover (rebuilding the real flow on v2) is
   user-driven; this plan only takes us to "v2 is verified equivalent."

## 9. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | 1.13.3 doesn't allow asset body to exit without `MaterializeResult` | Fallback: asset body yields `MaterializeResult` with no data_version (placeholder); harvest sensor reports a 2nd materialization with data_version. Dagster takes latest. — **Probe early (see §10 Q4)**. |
| R2 | `report_runless_asset_event` not in 1.13.3 | Harvest sensor uses `context.log_event(AssetMaterialization(...))` from within the sensor tick (still requires a run wrapper). Verified by `test_harvest_sensor.py` before plumbing it. |
| R3 | SQLite-on-NFS in real deployment | Whitepaper §3.1 already forbids NFS for Dagster backend; PHASE1_LIMITATIONS.md should mirror this for status DB. Real deploy keeps status DB on local disk. |
| R4 | Idempotency-key collision between Phase 1 dispatches (e.g. test reruns) | Trigger fingerprint includes upstream data versions; for "rerun same input" the key is intentionally the same (correct dedup); for "different input" the key differs. |
| R5 | Fabric worker on LSF node can't reach status DB (NFS not mounted there) | For Phase 1: insist status DB is on shared NFS path *readable + writable* from both orchestrator and LSF nodes; document in ONBOARDING.md. For Phase 2: PostgreSQL over network solves this. |
| R6 | LSF run client's lack of orphan recovery causes Phase 1 demo to hang if any worker crashes | Documented limitation (§5 L2); demo includes a fail-loud detection (`scripts/run_demo.py` times out after N minutes with diagnostic dump). |

## 10. Open questions (need answers before code)

1. **v1 freeze policy**: confirm decisions §1 row 3 — v1 stays as
   reference, no deletion. Yes/no.

2. **Branch strategy**: open a new branch (e.g.
   `claude/execution-fabric-phase1`) for v2 work? Or wait for PR #18
   to merge and then start fresh on `main`?

3. **`MY_FLOW_ROOT` analogue for v2**: in v1, `LIBERATE_DAG_ROOT` is the
   shared dir for SOURCES/work/out. v2 also needs a "status DB shared
   path" reachable from both orchestrator and LSF nodes. Convention:
   `execution_fabric/state/<flow_name>.db` works locally; production
   needs an env-vars-driven path on shared NFS. Confirm naming.

4. **Materialization production**: at v1 we observed `MaterializeResult`
   with `data_version` works via Pipes (D2 evidence). For v2 we need
   the asset body to NOT produce a materialization (so harvest is sole
   source) — verify which of these 1.13.3 paths works:
   - **A**: asset body returns `None` / does nothing; sensor uses
     `instance.report_runless_asset_event(AssetMaterialization(...))`
     to backfill.
   - **B**: asset body yields placeholder MaterializeResult (no
     data_version); harvest sensor yields a new MaterializeResult
     later; Dagster's "latest wins" semantics handle it.
   - **C**: asset body yields MaterializeResult with data_version that's
     somehow obtained synchronously (e.g. wait for harvest before
     returning) — this REGRESSES to blocking; reject.

   Recommend a 30-minute probe BEFORE writing harvest.py:
   ```bash
   python -c "from dagster import DagsterInstance, AssetKey, AssetMaterialization; \
     inst = DagsterInstance.ephemeral(); \
     inst.report_runless_asset_event(AssetMaterialization(asset_key=AssetKey('x'))); \
     print(list(inst.get_materialized_partitions(AssetKey('x'))))"
   ```

5. **Equivalence target for v2**: equivalence_v2 compares v2 to **v1
   spec_dagster** (not to hand-rolled `examples/converted/`). Confirm
   this is the right axis — v1 has already been D2-verified vs
   hand-rolled, so v2 → v1 → hand-rolled transitive equality is enough.

6. **Phase 1 effort estimate accuracy**: ~8.5 days assumes no surprises
   from 1.13.3 sensor APIs (§9 R1, R2). If R1 fallback path is needed,
   add ~1 day; if both R1 and R2 needed, add ~2 days.

## 11. Out-of-scope (deferred to Phase 2)

All of whitepaper §6 (orphan recovery / dispatch dedup / harvest
idempotency-via-DB) is explicitly Phase 2. **The harvest sensor's
internal idempotency (via cursor + at-least-once) IS Phase 1**, because
without it the sensor itself corrupts lineage on restart. The full
§6 contracts (active-state-aware UNIQUE upsert with conflict
resolution; bjobs-driven reaper; etc.) wait.

Also out of Phase 1: Kafka, PostgreSQL, worker pool, Control logic, UI,
event log (whitepaper §7).

## 12. Files this plan creates if approved as-is

If you approve and we proceed: 31 new files, 17 verbatim-copied files
(via `git cp`), 0 deleted files (v1 untouched). Net repo growth
~50 files. Estimated total LOC including tests: ~2500.
