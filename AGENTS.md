# Dagster-expert — agent map

Air-gapped Dagster 1.13.7 framework + librarian. Two minimal
personalities; one strategic doc; one worked example.

| Personality | Capability | What it does |
|---|---|---|
| `dagster-expert` | librarian over `database/dagster-1.13.7/` | Answer "what's the API for X in 1.13.7?", "how do I configure dagster.yaml on this air-gap deploy?" |
| `flow-cartographer` | migration coach | Walk users through `/WHITEPAPER.md §5` (TDD + Clean Code recipe for porting an existing pipeline onto the Execution Fabric) |

## Source of truth (single)

**`/WHITEPAPER.md`** is the strategic source of truth for everything
about how the framework works. Personalities point at it; they don't
duplicate it.

The framework code at **`execution_fabric/`** implements it:
- `framework/spec/`, `framework/assets/`, `framework/versioning/` — M1+M2
- `framework/sensor/{dispatch,harvest}.py` — M3 (replaces v1 reconcile/cascade)
- `framework/fabric/{status_db,file_lock,lsf_run_client}.py` — M4 (the
  Execution Fabric layer)
- `flows/liberate_char/{spec.yaml, script.py, fabric_worker.py}` — M5
  worked example

The 1.13.7 corpus at **`personalities/dagster-expert/database/dagster-1.13.7/`**
is the librarian's source for Dagster API claims. Every claim cites a
file there.

## Personality switching

Internal, no CLI. Tell the agent "switch to flow-cartographer" when
you start a migration; "switch to dagster-expert" when you need a
Dagster API lookup. Both `ROLE.md` files are injected at every turn
(via the `role-load` hook in All-Might-aware harnesses).

## Hard rules (both personalities)

1. **No Dagster API from training memory.** Cite `database/dagster-1.13.7/`
   or refuse.
2. **No private imports** (`dagster._core.*`, `_internal.*`, `_private.*`).
3. **Air-gap only**: no `uv` / `dg` / `pipx` / Cloud / Components / k8s
   / public PyPI / Docker registries / telemetry.
4. **tcsh-first** shell syntax (`setenv VAR value`); bash in parens.
5. **Absolute paths** (no `cd` chains).
6. **No destructive ops** (`run wipe`, `asset wipe`, dropping Postgres
   tables) without explicit user consent.

## What this repo is NOT

- A general-purpose Dagster tutorial (the 20-lesson `learn/` curriculum
  was retired 2026-06-12; the demo and v1 push-based architecture went
  with it).
- A bundle to ship to other projects via `/one-for-all` (the corpus
  references the framework, so they travel together or not at all).
- A reference for `dg` / Components / Dagster+ — `docs.dagster.io`
  remains the source for those; air-gap users use plain `dagster`.

## Contributing

- **API gap** discovered while answering: open
  `personalities/dagster-expert/memory/lessons_learned/_inbox/<ISO>-<user>.md`.
  Curator promotes to a new `docs/<topic>.md`.
- **Framework code**: open a PR against the relevant module in
  `execution_fabric/framework/`. Test coverage required for any code
  touching the fabric layer (especially harvest sensor).
- **WHITEPAPER.md changes**: discuss before landing. The whitepaper
  is the architecture contract; changes propagate to ROLE.md, the
  skill, and tests.
