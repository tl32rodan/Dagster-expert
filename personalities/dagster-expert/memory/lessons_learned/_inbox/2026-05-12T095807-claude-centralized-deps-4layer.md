---
allmight_journal: v1
type: lesson_learned
submitter: claude
created_at: 2026-05-12T09:58:07+00:00
tags: [architecture, solid, tdd, dagster, dep-management, partition-mapping, less-capable-agent]
---
# Centralize Dagster dep facts in one file; the Dagster idiom of scattered decorator args fails at production scale

## Observation

The user pushed back on my proposed plan where I had `CORNER_MERGE_STEPS`
as an enumerated set inside ROLE-style instructions. Their critique:

> 目前你的 code 與現存的 lessons 都沒有很乾淨地把 dep 集中定義
> 而是分散在各個 decorator 與 sub function 中

They were right. In lessons 09–12 and my initial draft:

- `@asset(deps=[AssetKey(...)])` — asset-level explicit deps
- `@asset(ins={"x": AssetIn(..., partition_mapping=...)})` — partition mapping
  per input
- Function signature (Style A): implicit by-name deps
- Factory functions: `_make_step_asset(step_type, prev_step)` switches on step
  type and adds different deps inline
- Custom `PartitionMapping` classes — encode cross-partition resolution
- `key_prefix=[lib]` — implicit grouping

Six places encoding parts of "what depends on what." Violates SRP (asset
functions describe both deps AND computation) and OCP (adding a rule means
editing N files).

## Pattern adopted

Four layers with strict import boundaries:

```
Layer 0: spec/        pure data — PartitionRule Protocol, DepEdge dataclass
Layer 1: rules/       one file per rule — frozen dataclass with emit_edges()
Layer 2: registry.py  DEPS = DepRegistry([rule1, rule2, ...])  ← SINGLE SOURCE
Layer 3: translator   PartitionRule → built-in StaticPartitionMapping
Layer 4: factory      reads registry, emits @asset decorations
```

Each `DepRule` is `emit_edges(library, step) -> Iterable[DepEdge]`. Each
`PartitionRule` is `resolve(downstream_branch) -> frozenset[str]`. Both
are frozen dataclasses, trivially unit-testable, hashable.

The translator enumerates all branches at definition time and pre-computes
`StaticPartitionMapping` (built-in, works with reconciliation). Custom
`PartitionMapping` subclasses are kept as a fallback adapter but not used.

## Layer-boundary enforcement (TDD)

`tests/test_layer_imports.py` greps source for `^\s*(?:from dagster|import
dagster)\b` and fails CI if any file in `spec/`, `rules/`, or `registry.py`
matches. Translator + factory MUST match. Mechanical, agent-friendly.

## What this enables

- **Changing a dep rule**: edit one file in `rules/`, no asset code touched.
- **Adding a new rule**: new file in `rules/`, register in `registry.py`,
  add test. Factory + asset bodies untouched.
- **Auditing**: `DEPS.edges_for(library, step)` returns the full edge set
  for inspection / pytest assertion. No need to load Dagster.
- **TDD speed**: Layer 0/1/2 tests are < 1 ms each, no Dagster import.
- **Less-capable-agent navigation**: each file < 200 LoC, predictable
  naming. `rules/parent_mirror.py` is the parent-mirror rule, only the
  parent-mirror rule, and the test for it is `tests/rules/test_parent_mirror.py`.

## Anti-pattern signals to watch for

- An `@asset` decorator with more than 3 args beyond `partitions_def` /
  `group_name` / `compute_kind`. Whatever's in there is dep info that
  should be in a rule.
- A factory function with `if step_type == "X": deps.append(...)` chains.
  Should be a rule that triggers on `step == "X"`.
- A `PartitionMapping` instance written inline at the asset call site.
  Should come through `to_partition_mapping(rule)`.

## Tension with Dagster's documented style

The four-layer pattern is **stricter** than the idiomatic Dagster style
in the docs (which has deps directly in the decorator). Document the
departure in the demo's README so future readers (human or agent) know
why.

For learning lessons (lesson 01–12) the inline idiom is fine — it teaches
the API surface. For production demos (`demo/`) the discipline is worth
it because production codebases have dozens of rules + cross-cutting
override needs (`deps_overrides.yaml`) that the inline idiom can't carry.

## References

- `personalities/dagster-expert/demo/scale-lib/pipelines/` (canonical implementation)
- `personalities/dagster-expert/demo/scale-lib/tests/test_layer_imports.py` (enforcement)
- `personalities/dagster-expert/demo/scale-lib/README.md` § Architecture
- `personalities/dagster-expert/memory/understanding/scale-lib-demo.md`
