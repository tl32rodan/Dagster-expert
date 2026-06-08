# lab13 · LSF integration via Pipes

**Time**: 60 min · **Prerequisites**: lessons 09 (Pipes), 12 (scaling)

## What this demonstrates

Brian's TSMC environment uses IBM LSF for job scheduling. This lesson
covers **two scale regimes** — the choice depends on run COUNT, not
run weight:

| Scale | Path | Why |
|---|---|---|
| ≤ hundreds of concurrent runs | **Part A** — asset body bsubs via `PipesSubprocessClient`; `DefaultRunLauncher` | orchestrator can fork one Python process per run; simpler; Pipes gives bidirectional event flow |
| >~thousands of concurrent runs | **Part B** — custom `LSFRunLauncher`; one Dagster run = one bsub = one LSF job | orchestrator CANNOT fork that many run processes; run worker must execute on an LSF node;run/event store must be shared Postgres |

Part A is the default and is what `skills/lsf-executor/SKILL.md` covers
in operator detail. Part B is for the large-scale regime where the
breaking point is process count on the orchestrator (NOT within-run
parallelism — see `STANDARD_USAGE.md §9c`).

## Pieces

```
13-lsf-integration/
├── pipelines/asset.py            ← Dagster @asset uses
│                                    PipesSubprocessClient + lsf_submit
├── scripts/python/lsf_submit.py  ← reusable wrapper:
│                                    assembles bsub flags, throttles
│                                    queue depth, forwards Pipes env
├── scripts/python/char_inner.py  ← runs on LSF node; opens
│                                    dagster_pipes, reports back
├── scripts/mock_lsf/bsub          ← local-dev bsub shim that
│                                    runs the inner command inline
└── _smoke.py                      ← end-to-end PASS in ~7s with mock
```

## Run it locally (with mock bsub)

```bash
cd ~/projects/.../learn/13-lsf-integration
python -m _smoke    # 6 partitions, 6 .lib + 6 .out logs
```

The smoke driver prepends `scripts/mock_lsf/` to `PATH` so
`bsub` resolves to the shim. On real LSF: just don't set PATH,
real bsub takes over.

## Run interactively

```bash
export PATH=$PWD/scripts/mock_lsf:$PATH
dagster dev -m pipelines
# open http://127.0.0.1:3000, click Materialize on a PVT cell
```

You'll see bsub-style log lines: `Job <NNNN> is submitted...`

## Real LSF migration

When you move to a real LSF host:
1. Drop the mock from PATH (don't `export PATH=...`)
2. Verify `bsub --help` resolves to real bsub
3. Possibly add site-specific defaults to `lsf_submit.py`:
   project code, queue, default walltime, memory tier

The asset code does NOT change.

## Six requirements covered

See `skills/lsf-executor/SKILL.md` for the verbose treatment.
Quick map:

| Requirement | Where it lives in code |
|---|---|
| (1) Log 紀錄 | `bsub -o/-e <NFS path>`; asset reads back into context.log |
| (2) 中斷機制 | `bsub -K` propagates Dagster terminate → bkill |
| (3) 狀態紀錄 | `bsub -K` exit code; or polling in async mode |
| (4) Env 繼承 | `lsf_submit.py --env-mode pipes-only/all/explicit` |
| (5) 平行跑 | Partitioned asset = N parallel bsubs (Dagster scheduling) |
| (6) Pending throttle | `lsf_submit.py --max-pending N` + Dagster queue concurrency |

## Part B — Scale-up: custom LSFRunLauncher

### When to choose Part B over Part A

A custom `RunLauncher` is **only** justified when **run COUNT** exceeds
what the orchestrator host can fork — typically when the sensor /
backfill driver issues >~thousands of concurrent `RunRequest`s, and
each becomes one Python run-worker process under `DefaultRunLauncher`.
At that point the orchestrator hits PID / RAM / fork-storm limits and
runs queue up not because LSF is full but because **the orchestrator
itself cannot start them**.

Pipes does NOT help here: Pipes changes what a *single asset body*
does, not how many runs exist. `STANDARD_USAGE.md §9c` lays out the
four levels of concurrency — Part A is level 1 (Pipes at level 4 of a
single run); Part B moves run execution off the orchestrator entirely.

If your `bjobs -u $USER` head-count rarely exceeds the hundreds during
the busiest waves, stay on Part A.

### Architecture — one Dagster run = one bsub = one LSF job

```
   ┌───────────────────────────────┐
   │  Orchestrator (login node)    │
   │  • dagster-webserver          │
   │  • dagster-daemon             │
   │  • LSFRunLauncher.launch_run  │ ← bsubs ONCE per run, NO Python fork
   │  • check_run_worker_health    │   (per-run bjobs from run_monitoring)
   └─────────────┬─────────────────┘
                 │  bsub  dagster api execute_run …
                 ▼
   ┌───────────────────────────────┐
   │  LSF compute node              │
   │  (one per Dagster RUN)         │
   │  • dagster api execute_run     │ ← in_process executor over this
   │    starts the run worker        │   run's batch of items
   │  • asset body executes here    │
   │  • (optional) same-node        │
   │    PipesSubprocessClient       │ ← only to collect EDA tool stdout;
   │    to call EDA tool             │   NEVER contains bsub (nested bsub)
   └─────────────┬─────────────────┘
                 │
                 ▼  writes event log + materializations
   ┌───────────────────────────────┐
   │  Shared Postgres (run/event/  │ ← single source of truth across
   │  schedule store; required)    │   orchestrator + remote workers
   └───────────────────────────────┘
```

The five-layer framework whitepaper §6.2 (in
`personalities/flow-cartographer/FIVE_LAYER_WHITEPAPER.md`) carries the
full design for the production integration; this lesson is the corpus
reference for the contract.

### The `RunLauncher` ABC contract

`pipelines/launcher.py` is the readable reference impl. The three
methods you implement:

| Method | Purpose | What this lesson does |
|---|---|---|
| `launch_run(LaunchRunContext)` | Submit one bsub wrapping `dagster api execute_run`; persist `lsf/job_id` to `run.tags` so later calls can find it | Builds bsub argv from `default_*` + `run.tags["lsf/*"]`; parses `Job <NNNN>` from bsub output; `add_run_tags` |
| `terminate(run_id)` | Read job_id from run.tags, `bkill` it | Calls `bkill` shim; returns True |
| `check_run_worker_health(run)` | Map LSF state to `WorkerStatus` for the `run_monitoring` daemon | `bjobs -a -o "stat exit_code" -noheader`; `PEND/RUN/PSUSP/USUSP/SSUSP→RUNNING`, `DONE→SUCCESS`, `EXIT→FAILED`, no-record→`UNKNOWN` |

Plus `supports_check_run_worker_health = True` (class attr) and the
`ConfigurableClass` round-trip (`inst_data`, `config_type`,
`from_config_value`).

### Pieces (Part B)

```
13-lsf-integration/
├── pipelines/
│   └── launcher.py                 ← reference LSFRunLauncher impl
├── scripts/mock_lsf/
│   ├── bsub                        ← already used in Part A
│   ├── bjobs                       ← NEW: sync mock returns DONE 0
│   └── bkill                       ← NEW: noop + echo
├── dagster_launcher_demo.yaml      ← NEW: $DAGSTER_HOME/dagster.yaml
│                                       template that selects this launcher
└── _smoke_launcher.py              ← NEW: validates mock shim behaviors
                                       (launcher integration is framework-
                                       level — see whitepaper §8)
```

### Running Part B locally

The mock shims smoke (no Dagster install needed):

```bash
python3 _smoke_launcher.py
# Expected: bsub-K runs inline, bjobs returns DONE 0, bkill noop,
# bjobs -p/-r return empty.
```

For real launcher integration (requires `pip install dagster==1.13.3`
from the wheelhouse + Postgres):

```bash
# 1. Set up a Part-B-specific DAGSTER_HOME
mkdir -p /tmp/dagster-13-launcher-home
cp dagster_launcher_demo.yaml /tmp/dagster-13-launcher-home/dagster.yaml

# tcsh:
setenv DAGSTER_HOME /tmp/dagster-13-launcher-home
setenv PATH $PWD/scripts/mock_lsf:$PATH
# (bash: export DAGSTER_HOME=/tmp/dagster-13-launcher-home; export PATH=...)

# 2. Start the daemon + webserver (uses our LSFRunLauncher via dagster.yaml)
dagster dev -w workspace.yaml
```

For production (real LSF + Postgres), drop the mock from PATH, replace
the SQLite default in `dagster_launcher_demo.yaml` with the Postgres
block from `learn/12-scaling/POSTGRES_MIGRATION.md`, and set
`DAGSTER_PG_PASSWORD` via env.

### Resource ceilings — the formula

`max_concurrent_runs` is no longer bounded by orchestrator host
capacity. The real bound is:

```
max_concurrent_runs = min(
    LSF_user_slot_limit,                                       # busers / bqueues -l
    floor((PG_max_connections - reserved) / conns_per_worker)  # usually binding
)
```

Each in_process run worker holds at least one event-log writer
connection for its lifetime. With PG `max_connections=200, reserved=40,
conns_per_worker=2` the ceiling is `~80`, regardless of how many LSF
slots the queue allows. **PgBouncer transaction pooling** (see
`learn/12-scaling/POSTGRES_MIGRATION.md:221-222`) is the
highest-leverage mitigation: with it, the LSF term becomes binding.

### The nested-bsub trap (reversed in Part B)

In Part A the asset body bsubs. In Part B the launcher bsubs and the
asset body MUST NOT bsub again — that would be nested bsub (each
Dagster run holds **two** LSF slots). The asset body in Part B runs
in-process on the LSF node where the run worker already lives;
shell-outs to the EDA tool go through a same-node `PipesSubprocessClient`
(no bsub in the command) or plain `subprocess.run`.

## Related

- `skills/lsf-executor/SKILL.md` — the operator-facing reference (Part A)
- `learn/09-real-flow/` — Pipes integration without LSF (base pattern)
- `learn/12-scaling/POSTGRES_MIGRATION.md` — required at Part B scale
- `personalities/flow-cartographer/FIVE_LAYER_WHITEPAPER.md` §6.2 — production framework integration of the same `LSFRunLauncher` contract
- `STANDARD_USAGE.md` §8 — the two-regime tradeoff in summary form
