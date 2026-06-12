# Dagster-expert — Execution Fabric for Dagster 1.13.7 on LSF

A self-contained framework for orchestrating large-scale EDA
characterization on an air-gapped TSMC workstation, layered on
**Dagster 1.13.7** + **LSF**. Three deliverables live here:

| What | Where |
|---|---|
| Strategic architecture white paper | [`WHITEPAPER.md`](WHITEPAPER.md) |
| Framework code + worked example | [`execution_fabric/`](execution_fabric/) |
| Dagster 1.13.7 air-gap corpus + one skill | [`personalities/dagster-expert/`](personalities/dagster-expert/) |

## What problem does this solve

EDA characterization runs 10k+ cell-level computations per release;
~1% need rerunning when an input changes. Dagster's `DataVersion` +
asset graph is the right tool for that incremental rerun. **But**
Dagster's push-based RunLauncher model can't dispatch 10k long-lived
run workers — the daemon's launch throughput becomes the bottleneck.

**The pivot**: Dagster owns *lineage*; we own *execution*.
- The asset body fires a non-blocking `bsub` and returns immediately.
- A separate fabric worker on the LSF node runs the script, computes
  the data_version from its output, and writes SUCCESS to a status DB.
- A harvest sensor reads status DB SUCCESS rows and reports them as
  AssetMaterializations to Dagster (via `report_runless_asset_event`).

Status DB: **SQLite Phase 1, PostgreSQL Phase 2**.

Full rationale + design in [`WHITEPAPER.md`](WHITEPAPER.md).

## Quick verification (end-to-end)

```tcsh
setenv DAGSTER_HOME /tmp/fabric-demo
setenv LIBERATE_DAG_ROOT /tmp/fabric-demo/dag
setenv FABRIC_USE_MOCK_BSUB 1
cd execution_fabric
setenv PYTHONPATH $PWD
python -m scripts.run_demo
```
(bash: `export DAGSTER_HOME=… ; export …`)

Expected (≤ 5 minutes):
- 9/9 SUCCESS rows in `$DAGSTER_HOME/fabric_status.db`
- 9/9 `AssetMaterialization` for `characterize` in Dagster's event log
- 9 `.ldb` files in `$LIBERATE_DAG_ROOT/out/`
- Banner: `Execution Fabric demo: PASS`

## Repo layout

```
WHITEPAPER.md                       # the strategic doc
execution_fabric/                   # the framework
  framework/
    spec/                           # M1: schema + loader
    assets/                         # M2: builder + partition/mapping helpers
    versioning/                     # content_hash
    sensor/                         # M3: dispatch + harvest sensors
    fabric/                         # M4: status_db, file_lock, lsf_run_client
    generator.py                    # spec → Definitions
    config/dagster.fabric.yaml      # DefaultRunLauncher + QueuedRunCoordinator
  flows/liberate_char/              # M5 worked example (3 pvt × 3 cell)
    spec.yaml, script.py
    fabric_worker.py                # runs on LSF node; reads .ldb digest
    _vendor/                        # mock bsub.py + mock liberate.py
  tests/                            # 52 tests, all green against 1.13.7
  scripts/run_demo.py               # end-to-end demo (9/9 partitions)

personalities/
  dagster-expert/                   # 1.13.7 librarian (one skill)
    database/dagster-1.13.7/docs/   # ARCHITECTURE, ASSETS_PARTITIONS,
                                    # SENSORS, RUN_LIFECYCLE, AIRGAP_DELTAS
    database/dagster-1.13.7/examples/  # 5 validated examples
    skills/dagster-1.13.7-airgap/   # mandatory-consult skill
  flow-cartographer/                # migration coach for /WHITEPAPER.md §5
```

## Audience

- **Engineers** porting an existing EDA pipeline (Perl/shell/Python +
  hand-rolled job scheduling) onto the framework. Start at
  [`WHITEPAPER.md` §5 Migration Plan](WHITEPAPER.md#5-migration-plan).
- **AI agents** (Minimax M2.5, Kimi K2.5, etc.) on air-gapped
  workstations. They invoke the librarian via the skill at
  `personalities/dagster-expert/skills/dagster-1.13.7-airgap/SKILL.md`
  before writing any `from dagster import …` line.

## Air-gap stance

- No internet at runtime.
- No `dg` / `uv` / Components / Dagster+ / Cloud / k8s.
- Wheelhouse pattern for pip (`pip install --no-index --find-links=~/wheelhouse X`).
- tcsh-first shell syntax in every example; bash equivalent in parentheses.

See [`personalities/dagster-expert/database/dagster-1.13.7/docs/AIRGAP_DELTAS.md`](personalities/dagster-expert/database/dagster-1.13.7/docs/AIRGAP_DELTAS.md)
for the explicit delta list against `docs.dagster.io`.

## Versioning

Pinned to Dagster **1.13.7**. Bump path: when the internal Dagster
moves to 1.14.x, the librarian curator creates `database/dagster-1.14.x/`
alongside 1.13.7 rather than mutating in place; consumers pin per
project.

## License

See [LICENSE](LICENSE). The corpus and framework contain no proprietary
or vendor-internal content; mock LSF binaries are stand-ins for the
real `bsub`/`bjobs`/`bkill`.
