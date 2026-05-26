<!-- all-might generated -->
# Coverage: 04 — Dependency Definition

Scope: the source flow's job / step / asset dependency model versus
Dagster 1.13.3's `@asset(deps=...)`, `AssetIn`, `AssetKey`, partitioned
deps, `MultiPartitionsDefinition`, and the Style-A-vs-B
implicit-vs-explicit dep conventions.

## Flow behavior (must cite from $FLOW_SRC)

Required reading paths (use `grep -rn "<keyword>" $FLOW_SRC`):
- Job / pipeline definition module (search `dependency`, `deps`,
  `parent`, `upstream`, `downstream`)
- Fan-in / fan-out logic (search `fan_in`, `merge`, `collect`,
  `gather`)
- Sparse-matrix / cross-partition module (search `partition`,
  `cross`, `matrix`, `corner`, `branch`)
- Data-passing convention (search `pass`, `arg`, `input`, `output`,
  file-based vs in-memory)

Expected behaviors (flow-side, to be confirmed by `$FLOW_SRC` reading):
- B1: Each unit-of-work declares upstream deps explicitly (DAG, not
  implicit ordering).
- B2: Some deps pass data through arguments (in-memory); others pass
  data through the filesystem (tools that write outputs to disk).
  The flow makes this distinction.
- B3: Cross-partition deps exist — a downstream unit consumes outputs
  from multiple upstream partitions (fan-in).
- B4: The flow uses graph-theory terminology consistent with MEMORY.md
  user prefs (parent_of / is_root / ancestors_of). The branch named
  `corner` keeps its literal name; its role is `root`.

## Dagster 1.13.3 corresponding API

Source:
- `personalities/dagster-expert/database/dagster-1.13.3/docs/style-a-vs-b.md`
- `personalities/dagster-expert/database/dagster-1.13.3/docs/cross-location.md`

Also see:
- `personalities/dagster-expert/learn/02-deps-and-lineage/README.md`
- `personalities/dagster-expert/learn/08-complex-deps/README.md`
- `personalities/dagster-expert/learn/09-real-flow/README.md`
- `personalities/dagster-expert/learn/10-branched-flow/README.md`

Public APIs / classes:
- `@asset(deps=[AssetKey(["prefix", "key"]), ...])` — Style B
  (explicit deps, no data pass) — cite `docs/style-a-vs-b.md`
- `@asset` with function-arg `AssetIn` — Style A (implicit deps via
  arg name; data passed through IO manager) — cite
  `docs/style-a-vs-b.md`
- `AssetKey(["nested", "path"])` — multi-segment keys for cross-
  location deps — cite `docs/cross-location.md`
- `MultiPartitionsDefinition` for sparse-matrix partition shapes —
  cite `docs/partitions.md`
- `context.partition_key.keys_by_dimension` for cross-partition fan-in
  — cite `learn/09-real-flow/README.md`

## Coverage criteria (covered only if ALL true)

- [ ] C1: For each flow dep type, the increment picks **Style A** (data
  passed) **or** **Style B** (deps= only, no data pass) and justifies
  the choice. Flow tools that write outputs to disk → Style B.
- [ ] C2: The flow's fan-in patterns are mapped onto either
  `MultiPartitionsDefinition` (sparse matrix) or
  `context.partition_key.keys_by_dimension` (cross-partition glob),
  with cardinality math shown per MEMORY.md user prefs.
- [ ] C3: Cross-code-location deps (if the flow has multi-location code
  servers) are mapped onto multi-segment `AssetKey(["loc", "key"])`,
  cite `docs/cross-location.md`.
- [ ] C4: The increment uses graph-theory terminology (`parent_of` /
  `is_root` / `ancestors_of`) for the abstraction layer per MEMORY.md
  user prefs. Domain labels like `corner_of` / `is_corner` are
  rejected in the abstraction; the literal branch name `corner` may
  keep its name only when referenced as a concrete branch.
- [ ] C5: Total leaf count is enumerated (branches × steps × cells ×
  PVTs × …) **before** the partition shape is chosen, per MEMORY.md
  "cardinality math first" pref.
- [ ] C6: No `dagster._core.*` / `_internal.*` / `_private.*` imports
  in any dep declaration. Public API only.

## Gap triggers (mechanical)

Each criterion is **covered** (the increment cites the mapping) or a
**gap**. An unaddressed gap is a `coverage-gap` finding (verify check 6
FAILs); a gap explicitly parked in `flow-model/_open_questions.yaml` is
acceptable, not a hard reject. Each remediation below is how to *cover*
the criterion — parking it as an open question is the documented
alternative.

- C1 gap → `coverage-gap 04.C1: dep style (A vs B) not chosen per flow
  dep type. Remediation: cite docs/style-a-vs-b.md and pick a style
  per flow dep with one-line justification.`
- C2 gap → `coverage-gap 04.C2: flow fan-in not mapped onto
  MultiPartitionsDefinition or keys_by_dimension. Remediation: show
  cardinality math + pick a partition shape.`
- C3 gap → `coverage-gap 04.C3: cross-location deps not mapped onto
  multi-segment AssetKey. Remediation: cite docs/cross-location.md
  and rewrite the AssetKey.`
- C4 gap → `coverage-gap 04.C4: domain terminology used in the
  abstraction. Remediation: rename to parent_of / is_root /
  ancestors_of per MEMORY.md user prefs.`
- C5 gap → `coverage-gap 04.C5: leaf cardinality not enumerated before
  the partition shape. Remediation: list (branches, steps, cells,
  PVTs, …) and compute the product first.`
- C6 gap → `coverage-gap 04.C6: private Dagster imports in dep
  declaration. Remediation: rewrite using public API; if the public
  API is missing, write a case study to memory/lessons_learned/_inbox/.`

## Evidence template

| Criterion | Flow source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $FLOW_SRC/... | docs/style-a-vs-b.md::... | covered / gap |
| C2 | $FLOW_SRC/... | docs/partitions.md::MultiPartitionsDefinition | covered / gap |
| C3 | $FLOW_SRC/... | docs/cross-location.md::... | covered / gap |
| C4 | (terminology check) | MEMORY.md::User Preferences | covered / gap |
| C5 | (cardinality math) | MEMORY.md::User Preferences | covered / gap |
| C6 | $FLOW_SRC/... | Shared hard rules §5 in ROLE.md | covered / gap |
