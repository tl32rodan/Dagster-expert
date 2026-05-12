# scale-lib demo — architecture & reusable patterns

Lives at `personalities/dagster-expert/demo/scale-lib/`. Production-shaped
Dagster 1.13.3 demo wrapping a 46-branch × 21-step characterization flow.
Not a lesson — a reference implementation when the user wants to build
production patterns from lesson 01–12 building blocks.

## Architecture: 4 layers with import-boundary enforcement

| Layer | Files                                     | Dagster import? |
|-------|-------------------------------------------|------------------|
| 0     | `pipelines/spec/` (5 files)               | ❌ |
| 1     | `pipelines/rules/` (7 files)              | ❌ |
| 2     | `pipelines/registry.py`                   | ❌ |
| 3     | `pipelines/translator.py`                 | ✅ |
| 4     | `pipelines/factory.py` + `definitions.py` | ✅ |

`tests/test_layer_imports.py` enforces with `re.compile(r"^\s*(?:from
dagster|import dagster)\b", re.MULTILINE)` grep — fails CI if any file
in Layer 0/1/2 imports Dagster.

**Why**: dep facts live in ONE place (`registry.py`). Asset bodies do
not encode dep policy. Less-capable agents can change behavior by
editing one rule file without touching `@asset` decorators.

## Rule pattern

Each rule is a frozen dataclass implementing `emit_edges(library, step)
-> Iterable[DepEdge]`. Examples:

```python
@dataclass(frozen=True)
class StepChainRule:
    def emit_edges(self, library, step):
        prev = prev_in_chain(step)
        if prev is not None:
            yield DepEdge(upstream_step=prev, partition_rule=SameBranch())

@dataclass(frozen=True)
class ParentMirrorRule:
    applies_to: frozenset[str] = field(default_factory=frozenset)
    def emit_edges(self, library, step):
        if step in self.applies_to:
            prev = prev_in_chain(step)
            if prev:
                yield DepEdge(upstream_step=prev,
                              partition_rule=ParentOfDownstream(include_self=False))
```

Registry merges multiple rules emitting edges to the same upstream into
`UnionOf(rules)`.

## PartitionRule abstraction

Pure data, resolves downstream-branch → set of upstream-branch keys:

```python
class PartitionRule(Protocol):
    def resolve(self, downstream_branch: str) -> frozenset[str]: ...

@dataclass(frozen=True)
class SameBranch:                    # → IdentityPartitionMapping
class FixedPartitions(keys=...):     # → SpecificPartitionsPartitionMapping
class RootBranch:                    # → resolves to tree root, any downstream
class ParentOfDownstream:            # → resolves to immediate parent in variant tree
    include_self: bool = True
    to_root: bool = False
class UnionOf(rules=...):            # composite
```

Translator (Layer 3) enumerates all 46 branches and emits a built-in
`StaticPartitionMapping(downstream_partition_keys_by_upstream_partition_key={...})`.

## Variant tree

Encoded in `config/branches.yaml` as `{branch: {family, parent}}`. The
`parent` field defines the graph-theoretic tree:

- ``corner`` is the unique root (no parent).
- Standard family (em, ht, lvf) → corner
- ``lvf_ht`` → ``lvf`` (the "_ht mirrors non-_ht" rule)
- mpwda variants → mpwda
- ``tmsf_self`` → corner; tmsf variants → tmsf_self
- ``tmsf_lde<N>`` → tmsf_self; ``tmsf_lde<N>_ht`` → tmsf_lde<N>

Pure functions in `pipelines/spec/branch_hierarchy.py`:
`parent_of`, `is_root`, `ancestors_of`, `descendants_of`, `roots`.

## Folder-as-asset contract (CONTRACT.md)

Tier 1 (Dagster) never reads file content. Per asset materialization:

1. Runner calls `subprocess.run([perl_or_python, script,
   --library, --branch, --step, --out])`.
2. Script writes whatever it wants under `--out`.
3. `folder_digest.digest_folder_manifest(out_dir)` hashes
   `(rel_path, size, int(mtime))` for every file — O(n) on stat().
4. Writes `<out>/.dagster_meta.json`.
5. Returns `MaterializeResult(data_version=DataVersion(digest), metadata={...})`.

LSF swap point in `runners.py`: change `subprocess.run([perl, ...])` to
`subprocess.run(["bsub", "-K", "-J", f"{step}-{branch}", perl, ...])`.
Rest of pipeline unchanged.

## Mock-script symlinks

`scripts/perl/_template.pl` is the only real perl file. All 17 step
scripts symlink to it: `step0.pl -> _template.pl`. Same for
`scripts/python/_template.py` and the 4 python step scripts. Real
binaries swap in one symlink at a time.

```bash
for s in step0 auto_download phantom BEpreQ step1 step6 step7 FunKits \
         rln trf cdk pgv apl spm mpwda_kit mtbf meta; do
  ln -sf _template.pl "$s.pl"
done
```

## Test pyramid

- 23 spec (pure logic, < 1 ms each)
- 38 rules (each rule isolated)
- 12 registry (rule composition, UnionOf merging)
- 10 translator (PartitionRule → Dagster PartitionMapping)
- 3 layer-imports (regex grep on source)
- 10 Definitions integration (asset count, dep edges, partition shape)
- 8 UI GraphQL (asset catalog, lineage, partition matrix, group_name,
  source observability) — skipped unless DAGSTER_GRAPHQL_URL is reachable

Total: 89 tests, ~1 s without UI, ~1 s with UI.

## Cardinality at production scale (per `cardinality_calc.py`)

```
DEMO         1 lib × 46 branches × 21 steps ≈ 1.1k partition records
PRODUCTION   1 lib × 64 branches × 21 steps ≈ 1.5k partition records
FUTURE 10×   1 lib × 460 branches × 21 steps ≈ 10.6k partition records
```

All three fit SQLite. PVT (3k+) and cell (~100) are Tier-2 / script-internal
and do NOT appear in this math.

## Source observation pattern (lesson 16 territory)

`@observable_source_asset` on PVT manifest + cell list:

```python
@observable_source_asset(name="pvt_manifest", group_name="sources")
def pvt_manifest_source():
    return DataVersion(sha256(_PVT_MANIFEST.read_bytes()).hexdigest()[:32])
```

Not currently wired as upstream of any step (cells / PVTs flow through
script CLI args, not through Dagster). Future hook point: add a DepRule
that emits an edge with `upstream_library=None, upstream_step="pvt_manifest"`.
