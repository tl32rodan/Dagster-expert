<!-- all-might generated -->
---
allmight_status: v1
last_activity: 2026-05-12T09:58:06+00:00
---
# dagster-expert — Status

## Active focus
Built `demo/scale-lib/` — production-shaped Dagster 1.13.3 demo wrapping
46-branch × 21-step characterization flow with 4-layer dep architecture
(spec / rules / registry / translator / factory). 89 tests pass; UI
verified via GraphQL.

## Recent topics
- 2026-05-12 scale-lib demo design + implementation (4-layer dep arch,
  Tier-1/Tier-2 boundary via folder digest, graph-theory terminology
  shift from "corner" to "parent / root", 89 tests + UI GraphQL ops)
- 2026-05-11 lesson 13-16 (LSF executor + scheduling + sensors + hooks)
- 2026-05-10 lesson 12 scaling beyond SQLite (cardinality math)

## Open threads
- AP `.ap_done` compatibility shim (sensor + post-step touch hook) in
  `demo/scale-lib/` — design described in CONTRACT.md, not yet implemented
- Second library code location for `demo/scale-lib/` (lesson 11 multi-loc
  pattern) — deferred until multi-library flow needed
- Real LSF bsub wiring on `PerlRunner.run` — one-line swap documented in
  `demo/scale-lib/pipelines/runners.py`; deferred until real binaries arrive
- Tier-2 nested Dagster experiment — deferred indefinitely; upgrade
  criteria in `demo/scale-lib/README.md`
- 1.13.4 corpus ingest — librarian has no 1.13.4 docs; user said focus on
  1.13.3; re-evaluate when release notes arrive
