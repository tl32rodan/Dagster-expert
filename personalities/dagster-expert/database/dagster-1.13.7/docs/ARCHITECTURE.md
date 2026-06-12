# ARCHITECTURE.md — Dagster 1.13.7 component model

Read this when the question is about **what process does what**, **where
state lives**, or **how processes connect** in a self-hosted Dagster
deployment.

## 1. The three long-lived processes

| Process | Started by | Responsibilities |
|---|---|---|
| **Code server** (gRPC) | `dagster api grpc -m <module>` or `workspace.yaml` | Hosts user code (assets, jobs, sensors, schedules); serves Definitions metadata to webserver + daemon via gRPC |
| **Daemon** | `dagster-daemon run -w workspace.yaml` | Ticks sensors + schedules; dequeues runs from coordinator; launches run workers via RunLauncher; runs `run_monitoring` health checks |
| **Webserver** | `dagster-webserver -w workspace.yaml` | UI + GraphQL; OPTIONAL — production sensor-only deploys (this codebase) skip it |

The daemon and webserver each connect to one or more code servers over
gRPC. Code servers can be local (auto-spawned) or remote (long-lived
service on another host).

## 2. Run worker — one per run

When the daemon launches a run, it spawns a **run worker** process.
The run worker's lifetime equals the run's lifetime. For
`DefaultRunLauncher`, this is a subprocess of the daemon's host. For
custom RunLaunchers (k8s, EC2, LSF), this is a remote process — but
1.13.7's design assumes the run worker reaches the same storage
backend (Postgres, S3 io_manager, etc.) the orchestrator uses.

> **WHY the Execution Fabric inverts this** (`/WHITEPAPER.md` §1.3):
> at 10k+ concurrent runs, the daemon cannot push that many run workers
> through `launch_run` fast enough. The fabric reduces Dagster runs to
> "short dispatch + harvest" and runs the actual computation on LSF
> nodes via its own non-blocking client.

## 3. Storage — instance state lives in `$DAGSTER_HOME`

`DagsterInstance` = `dagster.yaml` config + 4 storages:

| Storage | Default | What it holds |
|---|---|---|
| `run_storage` | SQLite at `$DAGSTER_HOME/history/runs.db` | Run records, run tags |
| `event_log_storage` | SQLite at `$DAGSTER_HOME/history/runs/<id>.db` | Per-run + global event log entries (incl. AssetMaterializations) |
| `schedule_storage` | SQLite at `$DAGSTER_HOME/schedules/schedules.db` | Schedule + sensor state, cursors, tick history |
| `compute_log_manager` | `LocalComputeLogManager` at `$DAGSTER_HOME/compute_logs/` | Captured stdout/stderr per run + step |

For multi-host deployments **switch storage to PostgreSQL** (one
backend, all four storages converge on it). Configure via
`dagster.yaml` `storage.postgres.postgres_db.…`. See
[AIRGAP_DELTAS.md §3](AIRGAP_DELTAS.md).

## 4. Run coordinator + run launcher (separable)

Two concepts often confused:

| | Where | Job |
|---|---|---|
| `RunCoordinator` | Daemon-side queue | Holds `RunRequest`s; releases at `max_concurrent_runs` + `tag_concurrency_limits` budget |
| `RunLauncher` | Per launched run | Starts a run worker; provides `terminate(run_id)` + `check_run_worker_health(run)` |

Standard combo for prod: `QueuedRunCoordinator` + `DefaultRunLauncher`.

```yaml
# dagster.yaml
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 64
    dequeue_use_threads: true
    dequeue_num_workers: 8
    tag_concurrency_limits:
      - key: "license/primetime"
        value: "true"
        limit: 4

run_launcher:
  module: dagster._core.launcher
  class: DefaultRunLauncher
```

## 5. `run_monitoring` daemon

When the launcher reports `supports_check_run_worker_health = True`,
the `run_monitoring` daemon periodically calls `check_run_worker_health`
and transitions stuck runs to FAILED. Tunables:

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 180   # NOT_STARTED → STARTED grace
  poll_interval_seconds: 60
```

## 6. Process topology — production (this codebase)

```
+---------------------+      gRPC      +------------------+
| dagster-daemon      | <------------> | code server(s)   |
|   - sensor daemon   |                | (your code)      |
|   - queued daemon   |                +------------------+
|   - monitoring      |
+---------------------+
        |  spawns run worker (DefaultRunLauncher)
        v
+---------------------+
| run worker (short)  |   asset body fires bsub to LSF (non-blocking)
|                     |   returns; harvest sensor later picks up SUCCESS
+---------------------+

Postgres ───── run/event/schedule store (shared by all of the above)
```

## 7. What lives where (cheat sheet)

| Need | Component |
|---|---|
| "Add an asset" | Code server (your module's @asset) |
| "Dispatch a run when X is missing" | Sensor → RunRequest → coordinator → launcher |
| "Run is stuck" | `run_monitoring` daemon + launcher's `check_run_worker_health` |
| "Restart all dagster procs without losing run state" | Storage at `$DAGSTER_HOME` (or Postgres) survives; processes are stateless |
| "Push state from sensor to a 3rd-party store" | Use sensor's `context.cursor`; sensor side effects survive daemon restart |

## 8. References

- 1.13.7 source: `github.com/dagster-io/dagster` at tag `1.13.7`
- This page's claims are validated by running 52 tests + an end-to-end
  demo against installed 1.13.7 (`execution_fabric/tests/` +
  `scripts/run_demo.py`).
