<!-- allmight_l1_cap=4096 -->
<!--
  L1 (MEMORY.md) is portable-only memory: what is true and useful no
  matter which corpus you work on. Keep it tight.
-->

# Project Memory

> **Default personality**: dagster-expert
> **Active personality**: dagster-expert

## Project map

| Personality | Capability | Scope |
|---|---|---|
| dagster-expert | librarian | Dagster **1.13.7** air-gap: API + architecture corpus. ONE skill: `skills/dagster-1.13.7-airgap`. |
| flow-cartographer | migration coach | Walks users through `/WHITEPAPER.md §5` (TDD + Clean Code recipe) for porting an existing pipeline onto the Execution Fabric. |

## User preferences

- Shell: **tcsh** (`setenv` first; bash `export` in parens).
- Air-gapped TSMC workstation: no internet at runtime, no public PyPI /
  Docker registries, no Dagster+ / Cloud / dg / uv / k8s.
- Primary agent runtime: **Minimax M2.5** (or similar). Instructions
  are mechanical / checklist-driven, not judgment-based.
- **Graph-theory terminology over domain-specific labels**:
  `parent_of` / `is_root` / `ancestors_of` rather than `corner_of` /
  `is_corner`.
- **Cardinality math first** when scaling: enumerate leaf count
  (branches × steps × cells × PVTs × …) before committing to a
  partition / asset design.
- **TDD + Clean Code** are the migration methodology (WHITEPAPER §5).

## Key facts

- Dagster version: **1.13.7**.
- Architecture: **Execution Fabric** = non-blocking LSF client + status
  DB + dispatch & harvest sensors. **No** custom RunLauncher; **no**
  in-asset Pipes.
- **Production storage = PostgreSQL.** SQLite is the in-repo reference
  adapter (single-host demo + tests). SQLite + `fcntl.flock` on NFS is
  a rejected design — see WHITEPAPER §11.
- Status DB execution truth ≠ Dagster materializations (the dispatch
  asset body auto-emits a placeholder materialization; dispatch sensor
  reads `observed` from status DB only).
- Strategic source of truth: `/WHITEPAPER.md` (root).
- Framework code: `execution_fabric/`.
- 1.13.7 corpus: `personalities/dagster-expert/database/dagster-1.13.7/`.

## Active goal

Verify the Execution Fabric framework end-to-end on real Postgres +
real LSF (>10k+ concurrent characterize runs). The in-repo SQLite
demo is a single-host smoke; production needs the user's existing
Postgres infrastructure plugged into a thin psycopg2 adapter behind
the same `status_db` API surface.

## Hard rules carried by both personalities

1. **No Dagster API from training memory.** Cite
   `database/dagster-1.13.7/` or refuse.
2. **No private imports** (`dagster._core.*` / `_internal.*` / `_private.*`).
3. **No `bsub` in `script.py`** — framework wraps via `lsf_run_client`.
4. **No SQLite + flock on NFS** — rejected design (WHITEPAPER §11).
5. **Air-gap only** — refuse `uv` / `dg` / Cloud / Components / k8s /
   public PyPI / Docker registries / telemetry.
6. **tcsh-first** shell syntax; bash in parens.
7. **Absolute paths** (no `cd` chains).
8. **No destructive ops** without explicit user consent.

## Persistent agent-design lessons

Captured 2026-05-11; carried across repo restructures because they're
about designing for less-capable agents, not about Dagster.

1. **Mechanical triggers over judgment.** Rewrite "always consult
   librarian first" as "before writing `^from dagster import`, run the
   mechanical lookup sequence; 0 results ⇒ REFUSE".
2. **Refusal as a feature.** Hard rules become refusals with exact
   remediation, not best-effort warnings.
3. **Visible state checkpoints.** Print `echo $DAGSTER_HOME`,
   `which dagster`, `dagster --version` at the start of multi-step
   tasks.
4. **Shell-aware command blocks.** tcsh `setenv` first; bash `export`
   in parens.
5. **Absolute paths only.** No `cd` chains.
6. **Pair every command with its verify command + expected output.**

## Architectural lessons (load-bearing)

- **Dagster lineage vs execution must be cleanly separated** for
  10k+ scale. Lineage stays in Dagster; execution lives in the
  Execution Fabric.
- **Status DB > Dagster materializations as execution truth.** The
  dispatch asset body returning None auto-emits a placeholder
  materialization; trusting that for `observed` stops redispatch
  before any real computation. Dispatch sensor reads status DB only.
- **Harvest sensor is the most fragile module.** Cursor advances
  ONLY after both `report_runless_asset_event` AND `mark_harvested`
  succeed. Partial failure preserves prefix, leaves suffix for next
  tick. Over-test (`test_harvest_sensor.py` has 6 cases).
- **Idempotency key = `sha256(asset, partition, sorted(upstream_data_versions))`.**
  One key serves three fault contracts (dispatch dedup, orphan
  recovery, harvest idempotency).
- **`define_asset_job` rejects mixed partition shapes** in one
  selection. Build one job per asset (or per shape).

See `/WHITEPAPER.md` for the full architecture.
