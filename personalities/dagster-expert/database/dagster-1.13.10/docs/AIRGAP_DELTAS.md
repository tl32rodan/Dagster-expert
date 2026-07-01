# AIRGAP_DELTAS.md — How our deploy differs from mainstream Dagster

Mainstream tutorials and `docs.dagster.io` assume internet, Dagster+,
modern toolchain, etc. This page lists the **deltas** that air-gap +
TSMC internal practices impose. Refer to this when an external doc
suggests something that won't work here.

## 1. Toolchain: NO `dg` / `uv` / `pipx`

| Doc says | We use |
|---|---|
| `dg dev` | `dagster dev -w /abs/workspace.yaml` |
| `dg list defs` | `dagster definitions list -w /abs/workspace.yaml` |
| `dg launch -j J` | `dagster job execute -w /abs/workspace.yaml -j J` |
| `dg materialize -s K` | `dagster asset materialize -w /abs/workspace.yaml --select K` |
| `uv add X` | `pip install --no-index --find-links=~/wheelhouse X` |
| `uv run …` | `python -m …` (inside activated venv) |
| `dg components add` | **Refuse.** Components is `dg`-only. Write plain `@asset`/`@op`. |

Wheelhouse path: `[~/wheelhouse/]` (resolve at session start).

## 2. NO Dagster+ / Cloud / Insights / agent

Features absent on this deploy:

- Cloud-hosted run history / lineage UI
- Insights (run cost + asset health analytics)
- Cloud sensors / serverless
- Branch deployments
- `dagster-cloud` CLI / Cloud agent

Equivalent OSS practices we use:
- Self-hosted Postgres for run/event/schedule storage
- The Execution Fabric's own status DB for execution truth
- `dagster-daemon run` only — webserver is optional and **not** used in
  production (`/WHITEPAPER.md §2.3`)

## 3. Storage: PostgreSQL, NOT SQLite-on-NFS

```yaml
# dagster.yaml — prod
storage:
  postgres:
    postgres_db:
      hostname: pg.internal
      port: 5432
      username: dagster
      password: { env: DAGSTER_PG_PASSWORD }
      db_name: dagster
```

Reasons:
- SQLite-on-NFS → alembic-locking conflicts under multi-process daemon
  + run workers → minute-long sensor ticks and intermittent
  `database is locked` errors.
- Cross-host: only Postgres can be reached by both the orchestrator
  daemon and LSF-node run workers.

Local-sim (dev only): SQLite default at `$DAGSTER_HOME` on local disk.

## 4. NO Kubernetes

The cluster is **LSF**, not k8s. So:

- No `K8sRunLauncher`, `dagster-k8s`, Helm charts
- The Execution Fabric uses LSF `bsub`/`bjobs`/`bkill` directly (mock
  versions for local-sim live in `execution_fabric/tests/_mock_lsf/`)

## 5. NO telemetry

```yaml
telemetry:
  enabled: false
```

Always set explicitly. Don't accept the default.

## 6. tcsh, not bash

User's session is **tcsh**. Show env-var setters as:

```tcsh
setenv DAGSTER_HOME /var/lib/dagster
setenv DAGSTER_PG_PASSWORD ...
```

with the bash equivalent in parentheses (`export DAGSTER_HOME=...`).

## 7. Absolute paths in commands

No `cd` chains. Every `dagster` invocation passes
`-w /abs/path/to/workspace.yaml` and `-m fully.qualified.module`.

## 8. Self-hosted gRPC code servers

Production runs code servers as long-lived processes on dedicated hosts:

```yaml
# workspace.yaml — prod
load_from:
  - grpc_server:
      host: code-pipelines.internal
      port: 4000
      location_name: pipelines
```

In dev, `python_module` auto-spawns the code server:

```yaml
# workspace.yaml — dev
load_from:
  - python_module:
      module_name: flows.liberate_char.definitions
```

## 9. Air-gap mock LSF binaries

For local-sim / CI without a real LSF cluster:

- Mock `bsub` (non-blocking detach):
  `execution_fabric/flows/liberate_char/_vendor/bin/bsub.py`
- Mock `bjobs`/`bkill` (test-only argv recording):
  `execution_fabric/tests/_mock_lsf/{bjobs,bkill}.py`

Invocation pattern from framework:
```python
build_definitions(..., bsub_bin="/abs/mock_bsub.py", invoker=[sys.executable])
```
The `invoker` lets the `.py` mock run without shebang/exec-bit setup.

## 10. NO `dagster._core.*` / `_internal.*` / `_private.*`

Reaching into private modules works briefly and breaks on the next
Dagster bump. If the public API can't do X, file an inbox case study
and propose a public-API workaround.
