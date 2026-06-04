# D1 build — lessons learned (2026-06-04)

Captured while actually building `spec_dagster` and running liberate-char
through it with **real Dagster 1.13.3** (installed in a venv) + **daemon +
reconcile sensor**. Each lesson is either (a) a concrete API gotcha a
weak agent would hit, or (b) a proposed edit to
`FIVE_LAYER_WHITEPAPER.md`. Lessons that warrant a whitepaper change are
tagged **[WP-EDIT]**.

---

## L1 — `from __future__ import annotations` breaks asset `context` validation  [WP-EDIT]

**Symptom.** `framework/assets/builder.py` started with
`from __future__ import annotations` (modern default) and annotated the
asset body `def _compute(context: AssetExecutionContext, ...)`. Dagster
1.13.3 raised:

```
DagsterInvalidDefinitionError: Cannot annotate `context` parameter with
type AssetExecutionContext. `context` must be annotated with
AssetExecutionContext, AssetCheckExecutionContext, OpExecutionContext,
or left blank.
```

**Cause.** PEP-563 turns every annotation into a *string*. Dagster's
`_validate_context_type_hint` resolves the `context` annotation to the
actual type; a string annotation (and equally a qualified
`dg.AssetExecutionContext`) defeats that resolution.

**Fix.** In the module that builds `@asset` functions: (1) do NOT use
`from __future__ import annotations`; (2) annotate `context` with the
**bare** `AssetExecutionContext` (import the symbol), never
`dg.AssetExecutionContext`, never a string.

**[WP-EDIT]** Whitepaper §4.3 shows `def _asset(context: dg.AssetExecutionContext, …)`.
That exact line fails. Change the §4.3 sample to import
`AssetExecutionContext` and annotate with the bare name, and add a one-line
caution: "asset-builder modules must not use `from __future__ import
annotations`; Dagster resolves the `context` annotation to a type."

## L2 — `MultiToSingleDimensionPartitionMapping` is BETA in 1.13.3  [WP-EDIT]

The single→multi cross-dimension mapping (the crux of liberate-char's
characterize deps) works, but every construction emits:

```
BetaWarning: Class `MultiToSingleDimensionPartitionMapping` is currently
in beta, and may have breaking changes in minor version releases…
```

**[WP-EDIT]** Whitepaper appendix A item 1 relies on this class without
noting its beta status. Add: "this primitive is **beta** in 1.13.3
(emits `BetaWarning`); pin the version and suppress the warning at the
framework boundary if it pollutes logs." It is the correct primitive —
just flag the stability caveat so an implementer isn't surprised.

## L3 — `Definitions.get_asset_graph()` → `resolve_asset_graph()`  [WP-EDIT]

Inspecting the built Definitions, `defs.get_asset_graph()` does not exist
in 1.13.3 — it is `defs.resolve_asset_graph()`. Minor, but the kind of
drift that stalls a weak agent.

**[WP-EDIT]** Any whitepaper / corpus inspection snippet that pokes at a
`Definitions` should use `resolve_asset_graph()` for 1.13.3.

## L4 — air-gap: invoke `dagster-daemon` by absolute venv path  [WP-EDIT]

The demo harness spawned `subprocess.Popen(["dagster-daemon", …])` and
got `FileNotFoundError` because only the venv *python* was on the
invocation path, not the venv *bin dir*. On an air-gapped box where the
venv isn't activated, the daemon CLI lives at
`<venv>/bin/dagster-daemon` (= `Path(sys.executable).parent`).

**[WP-EDIT]** Whitepaper §6 / onboarding §3.5 step 6 should show the
daemon launched with an absolute path (or an explicitly activated venv),
not a bare `dagster-daemon`, since the air-gap deployment won't have it
on `PATH` by default.

## L5 — the spec→generator path needs a generator-output contract  [WP-EDIT]

The whitepaper §4.3 sketches the compute asset body in detail but is
hand-wavy about the **generator** kind (it only really specifies the
`lsf_compute`/compute path). In practice the generator asset needs a
crisp contract for *what the script returns and who writes the files*.
The clean split that worked: **generator script functions return
`dict[abs_path, content]`; the framework writes the files and computes
the `content_hash` data_version over the concatenated content.** This
keeps the script pure (no Dagster, no MaterializeResult) and the
framework owns persistence + versioning.

**[WP-EDIT]** Add a §4.3 "generator kind" paragraph with this contract,
parallel to the compute-kind one. (The whitepaper's reference flow,
`netlist_files`, has only one compute asset and no generators, which is
why the gap went unnoticed; liberate-char has six generators and forced
the issue.)

---

## L6 — a headless daemon needs `default_status=RUNNING` on the sensor  [WP-EDIT]

With no webserver/UI on an air-gapped box, there is nothing to toggle a
sensor on. The daemon loads sensors **STOPPED** by default and evaluates
nothing. Setting `default_status=dg.DefaultSensorStatus.RUNNING` on the
built sensor makes `dagster-daemon run` evaluate it immediately. The
demo's `daemon.log` confirms it fired on the first tick:

```
SensorDaemon - Checking for new runs for sensor: characterize_reconcile_sensor
characterize_reconcile_sensor - characterize: 9 missing of 9 -> 9 RunRequests
SensorDaemon - Completed launch of run … for characterize_reconcile_sensor
```

**[WP-EDIT]** Whitepaper §5.3 `build_sensor` should set
`default_status=DefaultSensorStatus.RUNNING` (with a one-line note that
this is what makes the headless air-gap daemon evaluate it without a UI).

## L7 — the reconciliation model + QueuedRunCoordinator behaved exactly as designed

No change needed — recording the confirmation. `observed` via
`instance.get_materialized_partitions(AssetKey("characterize"))` (one
batched query, whitepaper §5.2) minus `desired` (the 9 partition keys)
produced 9 RunRequests; the `QueuedRunCoordinator` `tag_concurrency_limits`
(`liberate_run: 4`) drained them in waves — the poll saw **0 → 3 → 7 →
9**, never 9-at-once, proving the family cap works as the whitepaper §6
claims. Idempotency held: a second tick would see 9 observed and
`SkipReason`.

## L8 — bootstrap + daemon must share `DAGSTER_HOME` *and* `LIBERATE_DAG_ROOT`

The generators write SOURCES to `$LIBERATE_DAG_ROOT` and record
materializations in `$DAGSTER_HOME`; the daemon-launched characterize
runs read SOURCES from the same `$LIBERATE_DAG_ROOT` and the sensor reads
observed-state from the same `$DAGSTER_HOME`. A weak agent that lets these
diverge (e.g. bootstraps in one home, runs the daemon in another) gets a
silent empty-`observed` loop or missing-SOURCES failures. The harness
pins both env vars before either phase. Not a whitepaper change, but a
sharp edge worth the onboarding checklist (§3.5 already sets
`DAGSTER_HOME`; it should also pin the flow's data root).

---

## Daemon + sensor run — RESULT (verified)

```
[run_demo] bootstrapped 11 generator materializations + SOURCES on disk
[run_demo] daemon started
[run_demo] characterize materialized: 0/9
[run_demo] characterize materialized: 3/9      # QueuedRunCoordinator wave (cap=4)
[run_demo] characterize materialized: 7/9
[run_demo] characterize materialized: 9/9
[run_demo] RESULT: characterize partitions = 9/9
[run_demo]         runs total=23 succeeded=23
[run_demo]         artifacts: 9 .lib, 9 .ldb
[run_demo] determinism check tt_25/INV: daemon=6885155a1b11.. ref=6885155a1b11.. -> MATCH
[run_demo] D1 daemon+sensor verification: PASS
```

The 9 characterize leaves were driven entirely by the daemon's
SensorDaemon (see `.dagster_home/daemon.log`), executed by
`DefaultRunLauncher` under `QueuedRunCoordinator`, and the framework's
generated SOURCES fed the mock `liberate` to a **byte-identical content
digest** vs a direct reference run — i.e. behavioral equivalence on the
state-management + dependency aspects (whitepaper appendix C, C1/C4) for
this slice.

Reproduce: `PYTHONPATH=$PWD python -m scripts.run_demo` (dagster venv,
from `spec_dagster/`).
