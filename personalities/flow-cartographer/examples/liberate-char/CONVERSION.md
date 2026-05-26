# CONVERSION.md — charter for `liberate-char` (worked example, criteria MET)

A filled-in example charter. In a real run the user writes this and the
flow-cartographer loop executes it. Here it documents the conversion that
this folder already demonstrates end-to-end.

---

## Flow identity

**flow_name**: `liberate-char`

**`$FLOW_SRC`**: `../flow-src` (the hardcoded-path reference flow; read-only).
In a real run: `setenv FLOW_SRC /abs/path/to/flow` (tcsh) /
`export FLOW_SRC=/abs/path/to/flow` (bash).

**Build target**: `./converted` (this example's Dagster project).
`$DAGSTER_HOME` = a dedicated dir holding `dagster.yaml` (e.g. `~/.dagster-cartographer`).

---

## Goal (one sentence)

Model the Cadence Liberate characterization flow (per-PVT templates / section
tcls / model cards, per-cell netlists, a single cell list + main.tcl, a single
run.scr that runs `liberate` → `.lib`/`.ldb`) as a Dagster 1.13.3 asset graph
**partitioned on `pvt × cell`**, where every source is **generated from
`config/liberate.yaml`** (never copied, never read from `$FLOW_SRC` at runtime),
execution goes through **`bsub` via Pipes**, and the products are **provably
identical to the original except for embedded paths**.

---

## Steps in scope

1. `template_tcl` (per-PVT)  — generator
2. `section_tcl` (per-PVT, 6 sections) — generator (folder-as-asset)
3. `model_card` (per-PVT) — generator
4. `netlist` (per-cell) — generator
5. `cell_list`, `main_tcl` (single) — generator
6. `characterize` (per `pvt × cell`) — assemble per-leaf run.scr + run `liberate` via bsub/Pipes
- **Out of scope**: real Liberate tool integration (mocked here); a real LSF cluster (mock bsub).

---

## Partition & source strategy

- **Partition dimensions**: `pvt` and `cell` → a 2-axis
  `MultiPartitionsDefinition` (the 1.13.3 hard limit is 2). Partition key string
  is `cell|pvt` (dims sorted alphabetically), e.g. `INV|tt_25`.
- **Cross-dimension deps**: `characterize` (2D) depends on pvt-only sources
  (`template_tcl`, `section_tcl`, `model_card`) and the cell-only source
  (`netlist`) via `MultiToSingleDimensionPartitionMapping(partition_dimension_name=…)`;
  unpartitioned sources (`cell_list`, `main_tcl`) need no mapping.
- **Sources to extract (NOT copy)**: all of flow-src's per-PVT / per-cell files
  → `config/liberate.yaml` + `pipelines/generators.py`.
- **Cardinality math first**: 3 PVT × 3 cell = **9** characterize leaves;
  section files = 3 × 6 = 18. Small → SQLite is fine; no Postgres needed.
- **Tier boundary**: all steps are Tier-1 Dagster assets (folder-as-asset).

---

## Constraints (non-negotiable)

- Dagster **1.13.3** only; every symbol verified on the install + in the corpus.
- **No** `dagster._core/_internal/_private` imports.
- **Air-gap**: stdlib for flow-src/core; mock `liberate` + mock `bsub`; no network.
- Auto-rebuild via **`AutomationCondition.eager()`** (NOT the deprecated
  `AutoMaterializePolicy`); see `DO_AND_DONT.md`.
- LSF via **asset-body `bsub` + `QueuedRunCoordinator`** — no custom RunLauncher.
- Built-in partition mappings only; module-level singleton `partitions_def`.

---

## Success criteria (ralph-loop stop condition) — **all MET**

1. ✅ Every source is a generator (config + `generators.py`); no copied files.
2. ✅ `tests/test_generator_equivalence.py` proves byte-for-byte reproduction.
3. ✅ `python -m _smoke` materializes all 9 leaves through bsub/Pipes, exit 0.
4. ✅ `diff_proof` PASS: products differ from flow-src **only in embedded paths**.
5. ✅ Per-leaf rerun works (`dagster asset materialize --select characterize
   --partition 'INV|tt_25'`).
6. ✅ No `dagster._core/_internal/_private`; sensor + AutomationCondition wired.

### When met
This example is a frozen reference. For a real flow, point `$FLOW_SRC` at it,
copy this charter's shape, and let the loop drive build → verify → reflect.
