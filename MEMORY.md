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
| dagster-expert | librarian | Dagster **1.13.7** air-gap: API + architecture + usage corpus. ONE skill: `skills/dagster-1.13.7-airgap`. |
| flow-cartographer | migration coach | Walks users through `/WHITEPAPER.md §5` (TDD + Clean Code recipe) for porting an existing pipeline onto the Execution Fabric. |

## User preferences

- Shell: **tcsh** (use `setenv` first; show `export` in parentheses).
- Air-gapped TSMC workstation: no internet at runtime, no public PyPI /
  Docker registries, no Dagster+ / Cloud / dg / uv / k8s.
- Primary agent runtime: **Minimax M2.5** (or similar). Instructions
  are mechanical / checklist-driven, not judgment-based.
- **Graph-theory terminology over domain-specific labels**:
  `parent_of` / `is_root` / `ancestors_of` rather than `corner_of` /
  `is_corner`.
- **Cardinality math first** when scaling: enumerate the leaf count
  (branches × steps × cells × PVTs × …) before committing to a
  partition / asset design.
- **TDD + Clean Code** are the migration methodology (WHITEPAPER §5).

## Key facts

- Dagster version: **1.13.7** (was 1.13.3 through 2026-06-11; bumped).
- Architecture: **Execution Fabric** = non-blocking LSF client + status
  DB (SQLite Phase 1 → PostgreSQL+Kafka Phase 2) + dispatch & harvest
  sensors. **No** custom RunLauncher; **no** in-asset Pipes.
- Status DB execution truth ≠ Dagster materializations (the dispatch
  asset body's auto-emitted placeholder is misleading). The dispatch
  sensor reads `observed` from status DB only.
- Strategic source of truth: `/WHITEPAPER.md` (root).
- Framework code: `execution_fabric/`.
- 1.13.7 corpus: `personalities/dagster-expert/database/dagster-1.13.7/`.

## Active goal

Get the Execution Fabric framework verified at production scale
(>10k+ runs) on the real LSF cluster. Phase 2 (Postgres + Kafka +
reaper + worker pool) is the next milestone after Phase 1 (SQLite +
file lock) demo passes on multi-host LSF.

## Hard rules carried by both personalities

1. **No Dagster API from training memory.** Cite
   `database/dagster-1.13.7/` or refuse.
2. **No private imports** (`dagster._core.*` / `_internal.*` / `_private.*`).
3. **Air-gap only** — refuse `uv` / `dg` / Cloud / Components / k8s /
   public PyPI / Docker registries / telemetry.
4. **tcsh-first** shell syntax; bash in parens.
5. **Absolute paths** (no `cd` chains).
6. **No destructive ops** without explicit user consent.

## Lessons learned (carried over from earlier eras)

### Designing personalities for less-capable agents

Captured 2026-05-11; survived the 2026-06-12 restructure because they're
about agent design, not about Dagster.

1. **Mechanical triggers over judgment.** Rewrite "always consult
   librarian first" as "before writing `^from dagster import`, run the
   mechanical lookup sequence; 0 results ⇒ REFUSE".
2. **Refusal as a feature.** Hard rules become refusals with exact
   remediation, not best-effort warnings.
3. **Visible state checkpoints.** Print `echo $DAGSTER_HOME`, `which
   dagster`, `dagster --version` at the start of multi-step tasks.
4. **Shell-aware command blocks.** tcsh `setenv` first; bash `export`
   in parentheses.
5. **Absolute paths only.** No `cd` chains.
6. **Pair every command with its verify command + expected output.**

### Architecture design (added 2026-06-12)

- **Dagster lineage vs execution must be cleanly separated** for
  10k+ scale. Lineage stays in Dagster (DataVersion + harvest sensor);
  execution lives in the Execution Fabric (status DB + non-blocking
  LSF client + fabric worker).
- **Status DB > Dagster materializations as execution truth.** The
  dispatch asset body returning None auto-emits a placeholder
  materialization; trusting that for `observed` would stop redispatch
  before any real computation. Dispatch sensor reads status DB only.
- **Harvest sensor is the most fragile module.** Cursor advances ONLY
  after both `report_runless_asset_event` AND `mark_harvested` succeed.
  Partial failure preserves prefix, leaves suffix for next tick.
  Over-test this one (`test_harvest_sensor.py` has 6 cases).
- **Idempotency key = `sha256(asset, partition, sorted(upstream_data_versions))`.**
  One key serves three fault contracts (dispatch dedup, orphan
  recovery, harvest idempotency).
- **`define_asset_job` rejects mixed partition shapes** in one
  selection. Build one job per asset (or per shape).

See `/WHITEPAPER.md` for the full architecture.
