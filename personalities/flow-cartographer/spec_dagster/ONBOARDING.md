# Onboarding a flow on `spec_dagster`

> **Audience**: a **flow owner** (you have an EDA / batch pipeline and
> want it on the framework). You should NEVER need to edit anything
> under `framework/`.
>
> **Pre-reads**: `FIVE_LAYER_WHITEPAPER.md` §3 (spec schema), §3.5
> (concept-level SOP). This file is the *executable* companion: every
> step is paired with a **verify command** that exits 0 on success.
>
> **Reference flow**: `flows/liberate_char/` is the production-shaped
> example — read it in parallel with this doc. It is verified
> end-to-end by `scripts/run_demo.py` (daemon + sensor) and
> `scripts/equivalence.py` (behavioral equivalence vs the hand-rolled
> reference). See `flows/liberate_char/EQUIVALENCE.md`.

## 0. Prerequisites (one-time)

- A venv with `dagster==1.13.3` installed.
- `PYTHONPATH` includes the `spec_dagster/` root (so `framework` and
  `flows` are both importable).
- Shell is `tcsh` (use `setenv`); `bash` is shown in parentheses.

```tcsh
setenv PYTHONPATH /path/to/spec_dagster
# bash: export PYTHONPATH=/path/to/spec_dagster
```

**Verify**:

```tcsh
python -c "import dagster, framework; print(dagster.__version__)"
# expect: 1.13.3
```

## 0.5. Pick your dispatch mode

| Mode | When | dagster.yaml | Launcher | Storage |
|---|---|---|---|---|
| **local-sim** | dev; ≤ hundreds of concurrent runs | `framework/config/dagster.localsim.yaml` | `DefaultRunLauncher` | SQLite (single host) |
| **prod** | >~thousands of concurrent runs | (your own; see whitepaper §6.1) | `LSFRunLauncher` (`framework.launcher.lsf_run_launcher`) | Postgres (shared, see lesson 12) |

**Your `spec.yaml` and `script.py` DO NOT change between modes.** Only
`dispatch:` in spec and the `dagster.yaml` you pick differ. This is the
whole point of the framework.

---

## Step 1 — Scaffold the flow

```tcsh
cp -r flows/_template flows/my_flow
cd flows/my_flow
sed -i 's/TEMPLATE_change_me/my_flow/g' spec.yaml workspace.yaml
```

You now have: `spec.yaml`, `script.py`, `definitions.py`,
`workspace.yaml`, `README.md`. **You will edit only the first two.**

**Verify** (the scaffolded flow loads cleanly):

```tcsh
PYTHONPATH=<spec_dagster_root> python -c \
  "from framework.spec.loader import load_spec; load_spec('flows/my_flow/spec.yaml')"
# expect: exits 0; ValidationError prints the exact failing field
```

## Step 2 — Declare dimensions

Edit `spec.yaml`'s `dimensions:` block. Two kinds:

- `{ type: static, values: [...] }` — known up-front.
- `{ type: dynamic, source: <resource_key> }` — resolved at runtime.

Do a cardinality calculation BEFORE adding dimensions (MEMORY.md
preference): enumerate `dim1 × dim2 × ...` total leaf count. If it
crosses the thousands-of-runs line, you want `dispatch: lsf`.

**Verify**:

```tcsh
PYTHONPATH=<root> python -c "
from framework.spec.loader import load_spec
s = load_spec('flows/my_flow/spec.yaml')
print('dims:', list(s.dimensions))"
# expect: dims: [dim_a, ...]
```

## Step 3 — Declare assets

Each asset has a `kind`:

- `entry` — no script, no deps; an optional anchor for "everything below this".
- `generator` — light transformation. Its script returns
  `dict[abs_path: str, content: str]`. The framework writes each file
  and computes `content_hash` data_version (SHA256[:16] over the
  concatenated content).
- `compute` — the heavy lifter (typically shells out to an EDA tool /
  simulator). Its script returns `argv: list[str]`. The framework
  runs it via `PipesSubprocessClient`.

`depends_on` lists upstream `{asset, mapping}` pairs. `mapping` is an
*intent*, not Python:

| Intent | Translates to (whitepaper §4.1) |
|---|---|
| `"all"` | `AllPartitionMapping()` |
| `"last"` | `LastPartitionMapping()` |
| `"identity"` (same shape) | None (Dagster default identity) |
| `"identity"` (1D upstream → multi-D down, dim shared) | `MultiToSingleDimensionPartitionMapping(dim)` ⚠️ beta in 1.13.3 |
| `{dim: "identity"}` (single shared dim) | `MultiToSingleDimensionPartitionMapping(dim)` |
| unpartitioned upstream | None |

If a `compute` has `dispatch: lsf`, the spec's `lsf.default` (or the
asset's own `lsf:`) MUST set `queue` / `cores` / `mem_mb` / `walltime`.
The framework rejects the spec at load time if any required LSF field
is missing — you won't get a runtime mystery.

**Verify**:

```tcsh
PYTHONPATH=<root> python -c "
from flows.my_flow.definitions import defs
import dagster as dg
ag = defs.resolve_asset_graph()
print('assets:', sorted(k.to_user_string() for k in ag.get_all_asset_keys()))"
# expect: a sorted list including every asset.name you wrote
```

## Step 4 — Write `script.py` (pure functions)

Mechanical rules (every one of these is a real trap from the D1 build
— see `LESSONS.md`):

1. **Do NOT `import dagster`.** The framework wraps your functions;
   `script.py` must be pure-Python so it is unit-testable and
   transportable (same script runs in `dispatch: local` and
   `dispatch: lsf`).
2. **Do NOT use `@asset`, `@sensor`, `@job` decorators.**
3. **Do NOT `bsub` from a compute argv.** That is a nested bsub when
   `dispatch: lsf`; the launcher already wrapped the whole run in one
   bsub. The asset body runs IN-PROCESS on the LSF node — your tool
   call is just a plain subprocess from there.
4. **Generator signature**: `gen_fn(*partition_values)` → `dict[str, str]`
   (absolute paths to content). Partition values come in the order you
   declared in `partitioned_by:`.
5. **Compute signature**: `compute_fn(*partition_values)` → `list[str]`
   (argv). Return what to exec.

**Verify** the script is rule-clean:

```tcsh
grep -E "^(from|import) dagster" flows/my_flow/script.py
# expect: NO MATCHES (exits 1)
grep -E "@(asset|sensor|job|op)" flows/my_flow/script.py
# expect: NO MATCHES
grep -wE "bsub" flows/my_flow/script.py
# expect: NO MATCHES (your compute does not prepend bsub)
```

## Step 5 — `definitions.py` + `workspace.yaml`

Already scaffolded. **Leave as-is.** `definitions.py` is one line:
`defs = build_definitions(str(_FLOWS_ROOT))`. The framework discovers
your flow's `spec.yaml` via the scan.

**Verify** that the workspace loads and produces the expected jobs +
sensors:

```tcsh
PYTHONPATH=<root> python -c "
from flows.my_flow.definitions import defs
repo = defs.get_repository_def()
print('sensors:', [s.name for s in repo.sensor_defs])
print('jobs:', [j.name for j in repo.get_all_jobs() if not j.name.startswith('__')])"
# expect: one <compute_name>_reconcile_sensor per compute asset;
#         one <compute_name>_job per compute asset
```

## Step 6 — `DAGSTER_HOME`

```tcsh
# local-sim
setenv DAGSTER_HOME /local/dagster_home/my_flow_dev
mkdir -p $DAGSTER_HOME
cp framework/config/dagster.localsim.yaml $DAGSTER_HOME/dagster.yaml

# prod (LSF + Postgres): write your own dagster.yaml; see whitepaper §6.1
```

⚠️ DAGSTER_HOME must be **local disk** (NOT NFS) on the orchestrator —
SQLite-on-NFS locks; Postgres on shared host network is fine. See
whitepaper appendix A item 6.

**Verify**:

```tcsh
echo $DAGSTER_HOME
ls $DAGSTER_HOME/dagster.yaml
# expect: file exists
```

## Step 7 — Run

**Local dev (interactive UI)**:

```tcsh
dagster dev -w flows/my_flow/workspace.yaml
# open http://127.0.0.1:3000
```

**Daemon-driven (production-shaped)**:

```tcsh
# Use the venv-absolute dagster-daemon binary; not bare on PATH (L4).
setenv DAGSTER_DAEMON_BIN /path/to/venv/bin/dagster-daemon

# First: bootstrap any non-sensor-fed assets (typically the generators).
# liberate_char shows the pattern; scripts/run_demo.py is the template.

# Then start the daemon:
$DAGSTER_DAEMON_BIN run -w flows/my_flow/workspace.yaml &

# The reconcile sensor (default_status=RUNNING) will see N missing
# partitions on the next tick (every 30s) and emit N RunRequests.
# QueuedRunCoordinator drains them; tag_concurrency_limits caps your
# concurrency family (op_tags["dagster/concurrency_key"] in spec).
```

## Step 8 — Verify in flight + at rest

**Sensor fired** (daemon log):

```tcsh
grep "Checking for new runs for sensor:" $DAGSTER_HOME/daemon.log
grep "RunRequests" $DAGSTER_HOME/daemon.log
# expect: at least one tick logged; a "<name>: K missing of N -> K RunRequests" line
```

**Partition status** (programmatic):

```tcsh
PYTHONPATH=<root> python -c "
import dagster as dg
with dg.DagsterInstance.get() as inst:
    keys = inst.get_materialized_partitions(dg.AssetKey('my_compute'))
    print(f'materialized: {len(keys)}')"
```

**Data version chain** (whitepaper appendix B; LESSONS.md L11):

```tcsh
PYTHONPATH=<root> python -c "
import dagster as dg
with dg.DagsterInstance.get() as inst:
    recs = inst.get_event_records(
        event_records_filter=dg.EventRecordsFilter(
            event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=dg.AssetKey('my_compute'),
            asset_partitions=['<some_partition_key>'],
        ), limit=1, ascending=False)
    m = recs[0].event_log_entry.dagster_event.event_specific_data.materialization
    tags = m.tags or {}
    print('own data_version:', tags.get('dagster/data_version'))
    print('inputs:', {k.split('/',2)[2]: v for k,v in tags.items()
                      if k.startswith('dagster/input_data_version/')})"
```

---

## Mechanical traps (D1 lessons, baked into the framework)

These are already handled in `framework/`; you should not hit them.
But if you SEE one of these symptoms in unrelated code (e.g. your
own helper), they're the cause. Cross-reference `LESSONS.md`:

| # | Symptom | Cause | Lesson |
|---|---|---|---|
| 1 | `DagsterInvalidDefinitionError: Cannot annotate context parameter` | asset-builder module used `from __future__ import annotations`, or `context: dg.AssetExecutionContext` (qualified) | L1 |
| 2 | `BetaWarning: MultiToSingleDimensionPartitionMapping` in logs | beta primitive; behavior correct in 1.13.3 — pin version, suppress at framework boundary if noisy | L2 |
| 3 | `Definitions.get_asset_graph()` AttributeError | renamed to `resolve_asset_graph()` in 1.13.3 | L3 |
| 4 | `FileNotFoundError: 'dagster-daemon'` | bare cmd resolution; use absolute venv path | L4 |
| 5 | Headless daemon: sensor not firing | needs `default_status=DefaultSensorStatus.RUNNING` (no UI to toggle) | L6 |
| 6 | `TypeError: get_latest_materialization_event() got 'partition'` | wrong API in 1.13.3; use `get_event_records(EventRecordsFilter(asset_partitions=[pk]))` | L9 |
| 7 | `TypeError: EventRecordsFilter missing 'event_type'` | positional required; usually `DagsterEventType.ASSET_MATERIALIZATION` | L10 |
| 8 | `materialization.tags['dagster/logical_version']` returns None | renamed to `dagster/data_version` in 1.13.3 (plus `dagster/input_data_version/<upstream>`) | L11 |
| 9 | `instance.create_run_for_job(define_asset_job(...))` raises ParameterCheckError | `define_asset_job` returns `Unresolved`; use `@dg.job` for unit-test stand-ins | L13 |
| 10 | `AttributeError: launcher._instance has no setter` | property is read-only; call `launcher.register_instance(instance)` | L14 |

## Troubleshooting

**`load_all_specs` fails on a flow you weren't editing**: the framework
scans every `flows/*/spec.yaml`. Skip a directory by renaming it to
`_<name>` (leading underscore is the disabled-flow convention).

**Sensor evaluated but emitted 0 RunRequests, and 0 partitions
materialize**: check that your generators bootstrap actually wrote
events into the instance — the reconcile sensor reads
`instance.get_materialized_partitions(...)` for the compute asset, and
`desired - observed` is empty if observed somehow already equals
desired (or if the sensor is reading a different DAGSTER_HOME than your
bootstrap). Same `DAGSTER_HOME` for bootstrap + daemon is mandatory.

**`dispatch: lsf` but the worker can't reach Postgres**: prod uses
shared Postgres; the bsub `-env` list must forward `DAGSTER_PG_PASSWORD`
(framework does this — see `framework/launcher/lsf_run_launcher.py`),
and the LSF compute node must have a network route to your PG host
(`nc -zv pg.internal 5432` from a compute node).
