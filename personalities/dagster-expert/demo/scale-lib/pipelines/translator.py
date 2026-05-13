"""Layer 3 — translate ``spec.PartitionRule`` to Dagster ``PartitionMapping``.

This is the ONLY module (besides ``factory.py`` / ``definitions.py``)
that touches Dagster. ``test_layer_imports.py`` enforces that.

For each concrete PartitionRule in ``spec.partition_rule`` we have a
corresponding Dagster PartitionMapping subclass. The factory turns each
``DepEdge`` into an ``AssetIn`` whose ``partition_mapping`` field is
produced by ``to_partition_mapping(edge.partition_rule)``.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from collections import defaultdict

from dagster import (
    AssetDep,
    AssetIn,
    AssetKey,
    IdentityPartitionMapping,
    PartitionMapping,
    PartitionsDefinition,
    SpecificPartitionsPartitionMapping,
    StaticPartitionMapping,
)
from dagster._core.definitions.partitions.mapping.partition_mapping import (
    UpstreamPartitionsResult,
)
from dagster._core.definitions.partitions.subset.partitions_subset import (
    PartitionsSubset,
)

from .spec.branch_hierarchy import BranchHierarchy, default as default_hierarchy
from .spec.dep_edge import DepEdge
from .spec.partition_rule import (
    FixedPartitions,
    ParentOfDownstream,
    PartitionRule,
    RootBranch,
    SameBranch,
    UnionOf,
)


# ── custom mappings ─────────────────────────────────────────────────


class _PartitionRuleBackedMapping(PartitionMapping):
    """Generic adapter: turns a spec.PartitionRule into a PartitionMapping.

    The adapter delegates per-branch resolution to ``rule.resolve``.
    Both directions (upstream-for-downstream and downstream-for-upstream)
    iterate over partition keys — fine for static partitions up to a few
    thousand, which is well above the demo's scale.
    """

    def __init__(self, rule: PartitionRule, hierarchy: BranchHierarchy | None = None):
        self._rule = rule
        self._hierarchy = hierarchy or default_hierarchy()

    @property
    def description(self) -> str:  # pragma: no cover — Dagster surface only
        return f"PartitionRule({type(self._rule).__name__})"

    def get_upstream_mapped_partitions_result_for_partitions(
        self,
        downstream_partitions_subset: PartitionsSubset | None,
        downstream_partitions_def: PartitionsDefinition | None,
        upstream_partitions_def: PartitionsDefinition,
        current_time: Optional[datetime] = None,
        dynamic_partitions_store=None,
    ) -> UpstreamPartitionsResult:
        wanted: set[str] = set()
        if downstream_partitions_subset is None:
            # Materializing a non-partitioned downstream: resolve over all
            # downstream branches (rare for this demo, but keep correct).
            downstream_keys = list(self._hierarchy.all_branches())
        else:
            downstream_keys = list(downstream_partitions_subset.get_partition_keys())
        for k in downstream_keys:
            wanted.update(self._rule.resolve(k))

        upstream_keys = set(upstream_partitions_def.get_partition_keys(current_time))
        valid = wanted & upstream_keys
        missing = wanted - upstream_keys

        return UpstreamPartitionsResult(
            partitions_subset=(
                upstream_partitions_def.empty_subset().with_partition_keys(valid)
            ),
            required_but_nonexistent_subset=(
                upstream_partitions_def.empty_subset().with_partition_keys(missing)
            ),
        )

    def get_downstream_partitions_for_partitions(
        self,
        upstream_partitions_subset: PartitionsSubset,
        upstream_partitions_def: PartitionsDefinition,
        downstream_partitions_def: PartitionsDefinition,
        current_time: Optional[datetime] = None,
        dynamic_partitions_store=None,
    ) -> PartitionsSubset:
        # For each downstream key, ask "is any of my upstream picks in the
        # upstream_subset?". O(downstream × upstream) but tiny for ≤50 branches.
        upstream_keys = set(upstream_partitions_subset.get_partition_keys())
        triggered: set[str] = set()
        for down in downstream_partitions_def.get_partition_keys(current_time):
            if self._rule.resolve(down) & upstream_keys:
                triggered.add(down)
        return downstream_partitions_def.empty_subset().with_partition_keys(triggered)

    def validate_partition_mapping(self, *args, **kwargs):  # pragma: no cover
        # Validate is a no-op for our case; Dagster calls it during def load.
        return


# ── public API ──────────────────────────────────────────────────────


def _static_mapping_from_rule(
    rule: PartitionRule,
    hierarchy: BranchHierarchy | None = None,
    downstream_keys: frozenset[str] | None = None,
) -> StaticPartitionMapping:
    """Pre-compute a StaticPartitionMapping by enumerating every branch.

    Built-in StaticPartitionMapping works with Dagster's reconciliation
    and auto-materialize; the custom adapter does not. Since the demo's
    partition set is static (46 branches), enumeration is cheap.

    ``downstream_keys``: restrict the enumeration to those keys that
    actually exist in the downstream asset's PartitionsDefinition. Required
    when the downstream uses ``root_branch_partitions`` (1 key) while the
    upstream uses ``branch_partitions`` (46 keys) — otherwise the emitted
    mapping references downstream keys outside the downstream partition
    set and Dagster's validate_partition_mapping rejects with
    ``ValueError: mapping target partitions not in the downstream partitions definition``.
    """
    h = hierarchy or default_hierarchy()
    upstream_to_downstream: dict[str, set[str]] = defaultdict(set)
    for downstream in h.all_branches():
        if downstream_keys is not None and downstream not in downstream_keys:
            continue
        for upstream in rule.resolve(downstream):
            upstream_to_downstream[upstream].add(downstream)
    return StaticPartitionMapping(
        downstream_partition_keys_by_upstream_partition_key={
            up: sorted(downs) for up, downs in upstream_to_downstream.items()
        },
    )


def to_partition_mapping(
    rule: PartitionRule,
    *,
    downstream_keys: frozenset[str] | None = None,
) -> PartitionMapping:
    """Translate a spec PartitionRule into a Dagster PartitionMapping.

    ``downstream_keys``: if known, restrict the emitted mapping to the
    downstream asset's actual partition keys. Required for rules whose
    enumeration spans all branches but whose downstream uses a smaller
    partition set (e.g. ``root_branch_partitions``).

    Strategy:
    * ``SameBranch``      → ``IdentityPartitionMapping`` (built-in).
    * ``FixedPartitions`` → ``SpecificPartitionsPartitionMapping`` (built-in).
    * Everything else (``RootBranch``, ``ParentOfDownstream``, ``UnionOf``)
      pre-computes a ``StaticPartitionMapping`` (built-in) by enumerating
      branches. The adapter ``_PartitionRuleBackedMapping`` is kept as a
      fallback for unknown rules but is *not* the default — built-in
      mappings work with Dagster reconciliation; custom ones don't (see
      Dagster's deprecation notice).
    """
    if isinstance(rule, SameBranch):
        return IdentityPartitionMapping()
    if isinstance(rule, FixedPartitions):
        return SpecificPartitionsPartitionMapping(sorted(rule.keys))
    if isinstance(rule, RootBranch):
        return SpecificPartitionsPartitionMapping(sorted(rule.resolve("any")))
    if isinstance(rule, (ParentOfDownstream, UnionOf)):
        return _static_mapping_from_rule(rule, downstream_keys=downstream_keys)
    raise UnsupportedRule(rule)


class UnsupportedRule(TypeError):
    pass


def to_asset_dep(
    library: str,
    edge: DepEdge,
    *,
    downstream_keys: frozenset[str] | None = None,
) -> AssetDep:
    """Convert a DepEdge into an ``AssetDep`` for the ``deps=[...]``
    list of an ``@asset`` decorator.

    Using ``deps=`` (not ``ins=``) means Dagster tracks lineage and
    cross-partition mapping without trying to load upstream values into
    the asset body — which matches the folder-as-asset contract
    (Tier 1 doesn't read upstream output content; the runner / script
    fan-outs do).

    ``downstream_keys``: the partition keys of the asset that owns this
    AssetDep. Forwarded to ``to_partition_mapping`` so the emitted
    StaticPartitionMapping doesn't reference keys outside the downstream
    partition set.
    """
    # For source-asset deps (upstream_library=None AND single-part key),
    # we don't add the library prefix — sources have flat top-level keys
    # like AssetKey(["pvt_manifest"]). Convention: edge.upstream_library
    # of None + a step name matching a known source asset means "source".
    if edge.upstream_library is None and edge.upstream_step in _SOURCE_ASSET_NAMES:
        key = AssetKey([edge.upstream_step])
    else:
        upstream_lib = edge.upstream_library or library
        key = AssetKey([upstream_lib, edge.upstream_step])

    # partition_rule=None means upstream is unpartitioned (a source).
    # Omit partition_mapping; Dagster defaults to AllPartitionMapping for
    # unpartitioned upstream → partitioned downstream, which is what we
    # want (every partition of downstream observes the source).
    if edge.partition_rule is None:
        return AssetDep(asset=key)

    return AssetDep(
        asset=key,
        partition_mapping=to_partition_mapping(
            edge.partition_rule, downstream_keys=downstream_keys,
        ),
    )


# Source asset names that have flat (single-element) AssetKeys.
# Defined in pipelines/source_observers.py; keep in sync.
_SOURCE_ASSET_NAMES = frozenset({"pvt_manifest", "cell_list"})


def to_asset_in(
    library: str,
    edge: DepEdge,
    *,
    input_name: str | None = None,
) -> tuple[str, AssetIn]:
    """Legacy: convert to AssetIn (kept for unit tests; prefer
    ``to_asset_dep`` in the factory).
    """
    upstream_lib = edge.upstream_library or library
    key = AssetKey([upstream_lib, edge.upstream_step])
    if input_name is None:
        prefix = (
            f"in_{edge.upstream_library}_"
            if edge.upstream_library and edge.upstream_library != library
            else "in_"
        )
        input_name = f"{prefix}{edge.upstream_step}"
    return input_name, AssetIn(
        key=key,
        partition_mapping=to_partition_mapping(edge.partition_rule),
    )
