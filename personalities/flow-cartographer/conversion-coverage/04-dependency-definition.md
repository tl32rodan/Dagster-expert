<!-- all-might generated -->
# Audit: 04 — Dependency Definition

Scope: AP's job / step / asset dependency model versus Dagster
1.13.3's `@asset(deps=...)`, `AssetIn`, `AssetKey`, partitioned deps,
`MultiPartitionsDefinition`, and the Style-A-vs-B implicit-vs-explicit
dep conventions.

## AP behavior (must cite from $AP_SRC)

Required reading paths (use `grep -rn "<keyword>" $AP_SRC`):
- Job / pipeline definition module (search `dependency`, `deps`,
  `parent`, `upstream`, `downstream`)
- Fan-in / fan-out logic (search `fan_in`, `merge`, `collect`,
  `gather`)
- Sparse-matrix / cross-partition module (search `partition`,
  `cross`, `matrix`, `corner`, `branch`)
- Data-passing convention (search `pass`, `arg`, `input`, `output`,
  file-based vs in-memory)

Expected behaviors (AP-side, to be confirmed by `$AP_SRC` reading):
- B1: Each unit-of-work declares upstream deps explicitly (DAG, not
  implicit ordering).
- B2: Some deps pass data through arguments (in-memory); others pass
  data through the filesystem (tools that write outputs to disk).
  AP makes this distinction.
- B3: Cross-partition deps exist — a downstream unit consumes outputs
  from multiple upstream partitions (fan-in).
- B4: AP uses graph-theory terminology consistent with MEMORY.md
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

## Parity criteria (PASS only if ALL true)

- [ ] C1: For each AP dep type, the plan picks **Style A** (data
  passed) **or** **Style B** (deps= only, no data pass) and justifies
  the choice. AP tools that write outputs to disk → Style B.
- [ ] C2: AP fan-in patterns are mapped onto either
  `MultiPartitionsDefinition` (sparse matrix) or
  `context.partition_key.keys_by_dimension` (cross-partition glob),
  with cardinality math shown per MEMORY.md user prefs.
- [ ] C3: Cross-code-location deps (if AP has multi-location code
  servers) are mapped onto multi-segment `AssetKey(["loc", "key"])`,
  cite `docs/cross-location.md`.
- [ ] C4: The plan uses graph-theory terminology (`parent_of` /
  `is_root` / `ancestors_of`) for the abstraction layer per MEMORY.md
  user prefs. Domain labels like `corner_of` / `is_corner` are
  rejected in the abstraction; the literal branch name `corner` may
  keep its name only when referenced as a concrete branch.
- [ ] C5: Total leaf count is enumerated (branches × steps × cells ×
  PVTs × …) **before** the partition shape is chosen, per MEMORY.md
  "cardinality math first" pref.
- [ ] C6: No `dagster._core.*` / `_internal.*` / `_private.*` imports
  in any dep declaration. Public API only.

## Refusal triggers (mechanical)

- C1 unmet → `REJECT: 04.C1: dep style (A vs B) not chosen per AP
  dep type. Remediation: cite docs/style-a-vs-b.md and pick a style
  per AP dep with one-line justification.`
- C2 unmet → `REJECT: 04.C2: AP fan-in not mapped onto
  MultiPartitionsDefinition or keys_by_dimension. Remediation: show
  cardinality math + pick a partition shape.`
- C3 unmet → `REJECT: 04.C3: cross-location deps not mapped onto
  multi-segment AssetKey. Remediation: cite docs/cross-location.md
  and rewrite the AssetKey.`
- C4 unmet → `REJECT: 04.C4: domain terminology used in the
  abstraction. Remediation: rename to parent_of / is_root /
  ancestors_of per MEMORY.md user prefs.`
- C5 unmet → `REJECT: 04.C5: leaf cardinality not enumerated before
  the partition shape. Remediation: list (branches, steps, cells,
  PVTs, …) and compute the product first.`
- C6 unmet → `REJECT: 04.C6: private Dagster imports in dep
  declaration. Remediation: rewrite using public API; if the public
  API is missing, write a case study to memory/lessons_learned/_inbox/.`

## Evidence template

| Criterion | AP source (path:line) | Dagster reference | Status |
|---|---|---|---|
| C1 | $AP_SRC/... | docs/style-a-vs-b.md::... | PASS / FAIL |
| C2 | $AP_SRC/... | docs/partitions.md::MultiPartitionsDefinition | PASS / FAIL |
| C3 | $AP_SRC/... | docs/cross-location.md::... | PASS / FAIL |
| C4 | (terminology audit) | MEMORY.md::User Preferences | PASS / FAIL |
| C5 | (cardinality math) | MEMORY.md::User Preferences | PASS / FAIL |
| C6 | $AP_SRC/... | Shared hard rules §5 in ROLE.md | PASS / FAIL |
