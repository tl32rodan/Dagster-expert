# Dagster 1.13.7 corpus — INDEX

Curated subset of the official Dagster docs (`docs.dagster.io` /
`github.com/dagster-io/dagster` @ `1.13.7`), filtered for air-gap usage.

## What's in here vs. what's NOT

**IN** — covered explicitly (see files below).

**OUT** (with reason):

| Out of scope | Why |
|---|---|
| Dagster+ / Cloud / Insights | Air-gap (no SaaS) |
| `dg` CLI / `uv` / Components | Internal flow uses plain `dagster` + `pip --no-index` |
| Kubernetes / Helm / `K8sRunLauncher` | LSF cluster, not k8s |
| Dagster UI | Production runs daemon only; UI replaced by self-built reader on status DB (see `/WHITEPAPER.md` §2.3) |
| Full API reference | Use `pydoc dagster.<symbol>` at runtime; corpus covers concepts |
| Tutorials / quickstarts / homepage marketing | Architecture rigor only |

## Routing table

| Question | Read |
|---|---|
| "What is the daemon? Run worker? Code server?" | [ARCHITECTURE.md](ARCHITECTURE.md) |
| "How do I declare an asset with partitions?" | [ASSETS_PARTITIONS.md](ASSETS_PARTITIONS.md) |
| "Partition mapping: how does MultiToSingleDimension work?" | [ASSETS_PARTITIONS.md](ASSETS_PARTITIONS.md) §4 |
| "How do I write a sensor with cursor?" | [SENSORS.md](SENSORS.md) |
| "How does `report_runless_asset_event` work?" | [SENSORS.md](SENSORS.md) §3 |
| "What are the run states / RunLauncher contract?" | [RUN_LIFECYCLE.md](RUN_LIFECYCLE.md) |
| "Why not Components / dg / uv / k8s / SQLite-on-NFS?" | [AIRGAP_DELTAS.md](AIRGAP_DELTAS.md) |
| "What changed from 1.13.3 to 1.13.7?" | [1_13_7_RELEASE_NOTES.md](1_13_7_RELEASE_NOTES.md) |

## Examples

| File | Concept |
|---|---|
| `examples/01_asset_and_partitions.py` | Minimal multi-partition asset |
| `examples/02_sensor_with_cursor.py` | Sensor + cursor + idempotent action |
| `examples/03_runless_asset_event.py` | `report_runless_asset_event` from a sensor (harvest pattern miniature) |
| `examples/04_partition_mapping.py` | `MultiToSingleDimensionPartitionMapping` (used by the Execution Fabric) |
| `examples/05_run_lifecycle.py` | Minimal demo of run states + DefaultRunLauncher |

Each example is validated by `dagster definitions validate -m examples.<NN>_<topic>`
against an installed 1.13.7.

## Mandatory consult sequence (the skill enforces this)

For any answer that requires writing `from dagster import …`:

1. Check this `INDEX.md` for the topic.
2. Read the named `docs/<topic>.md`.
3. Read the matching `examples/<NN>_<topic>.py`.
4. If steps 1–3 yield zero matches, **REFUSE**. Don't fall back to
   training memory; Dagster's API drifts across minors and getting it
   wrong on an air-gap deploy means a failed reboot, not a stack trace.
5. Open `memory/lessons_learned/_inbox/<ISO>-<user>.md` describing the
   gap; user (Brian) curates into a new docs entry.
