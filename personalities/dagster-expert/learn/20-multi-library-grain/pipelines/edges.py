"""Compute upstream edges for each step.

This is the distilled equivalent of scale-lib's
``registry.py + rules/* + translator.py`` layers. Each step gets a list
of ``(upstream_step, PartitionMapping)`` pairs; the factory turns each
pair into an ``AssetDep`` keyed by the same library.

Five dep patterns are modeled:

1. **SetupGate** — every non-setup step depends on ``step0`` at the
   root branch (``corner``).
2. **StepChain** — chain steps (``step3..step6``) depend on the previous
   chain step at the same branch (Identity).
3. **ParentMirror** — at step5 (configurable via ``DEFAULT_PARENT_MIRROR_STEPS``),
   each downstream branch additionally reads its variant-tree parent's
   step4 output. Merged with the chain edge into one StaticPartitionMapping
   that maps upstream ``b`` → downstream {``b``, ``children_of(b)``}.
4. **Step7Follow** — ``step7`` depends on ``step1`` at the same branch.
5. **KitStep6Gate** — kits (except ``rln``) depend on ``step6`` at the
   root branch.

Cross-library edges are intentionally not modeled here (each library is
independent); add them in ``factory.py`` if you need them.
"""
from __future__ import annotations

from dagster import (
    IdentityPartitionMapping,
    PartitionMapping,
    SpecificPartitionsPartitionMapping,
    StaticPartitionMapping,
)

from .branches import all_branches, parent_of, roots
from .steps import (
    DEFAULT_PARENT_MIRROR_STEPS,
    kits,
    prev_in_chain,
    setup_steps,
)


# ── Pre-computed mappings (built once at module import) ─────────────


def _root_only_mapping() -> SpecificPartitionsPartitionMapping:
    """Maps every downstream partition to upstream ``corner`` (root).

    Used for:
    - SetupGate (any step → step0[corner])
    - KitStep6Gate (kit → step6[corner])

    Works whether the downstream uses ``branch_partitions`` (46 keys) or
    ``root_branch_partitions`` (1 key).
    """
    return SpecificPartitionsPartitionMapping(sorted(roots()))


def _parent_mirror_static_mapping() -> StaticPartitionMapping:
    """Encodes chain (``SameBranch``) ∪ parent-mirror (``ParentOf``) as a
    single ``StaticPartitionMapping``.

    For each downstream branch ``b``:
        wanted_upstream = {b} ∪ ({parent_of(b)} if not root)

    Then invert to ``{upstream: [downstream_keys_that_need_it]}``.

    Why pre-computed StaticPartitionMapping and not a custom
    PartitionMapping subclass: Dagster 1.13.3 deprecates custom mappings
    for reconciliation. Built-in ``StaticPartitionMapping`` (with the
    full enumeration) is the reliable path; it's tiny here (46 branches
    × 2 wanted = 92 entries).
    """
    upstream_to_downstream: dict[str, set[str]] = {}
    for downstream in all_branches():
        wanted: set[str] = {downstream}
        p = parent_of(downstream)
        if p is not None:
            wanted.add(p)
        for up in wanted:
            upstream_to_downstream.setdefault(up, set()).add(downstream)
    return StaticPartitionMapping(
        downstream_partition_keys_by_upstream_partition_key={
            up: sorted(downs) for up, downs in upstream_to_downstream.items()
        },
    )


# Memoize: same mapping reused across all 100 libraries × step5 edges.
_ROOT_ONLY = _root_only_mapping()
_PARENT_MIRROR = _parent_mirror_static_mapping()


# ── Public API ──────────────────────────────────────────────────────


def edges_for(step_name: str) -> list[tuple[str, PartitionMapping]]:
    """Return ``[(upstream_step, partition_mapping), ...]`` for one step.

    Order is purely cosmetic — Dagster reduces deps to a set. The order
    chosen here is rough dep-priority (setup gate first, chain second,
    follow / kit-gate last) which makes UI lineage edges easier to read.
    """
    out: list[tuple[str, PartitionMapping]] = []

    # 1. Setup gate — every non-setup step waits on step0[corner].
    if step_name not in setup_steps():
        out.append(("step0", _ROOT_ONLY))

    # 2. Chain dep.
    prev = prev_in_chain(step_name)
    if prev is not None:
        if step_name in DEFAULT_PARENT_MIRROR_STEPS:
            # chain + parent-mirror merged into one StaticPartitionMapping.
            out.append((prev, _PARENT_MIRROR))
        else:
            # Plain same-branch chain.
            out.append((prev, IdentityPartitionMapping()))

    # 3. step7 follows step1 (same branch).
    if step_name == "step7":
        out.append(("step1", IdentityPartitionMapping()))

    # 4. kits (except rln) wait on step6 of the root branch.
    if step_name in kits() and step_name != "rln":
        out.append(("step6", _ROOT_ONLY))

    return out
