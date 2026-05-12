"""Partition-resolution rules. ``PartitionRule`` is the abstract
contract: given a downstream branch, return the set of upstream
branches that must be present.

Each concrete rule is a frozen dataclass — value equality, hashable,
trivially testable.

The translator layer maps these to Dagster ``PartitionMapping`` instances.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from .branch_hierarchy import BranchHierarchy, default as default_hierarchy


@runtime_checkable
class PartitionRule(Protocol):
    """Pure: given downstream branch, return required upstream branches."""

    def resolve(self, downstream_branch: str) -> frozenset[str]: ...


@dataclass(frozen=True)
class SameBranch:
    """The default: depend on the matching upstream partition (Identity)."""

    def resolve(self, downstream_branch: str) -> frozenset[str]:
        return frozenset({downstream_branch})


@dataclass(frozen=True)
class FixedPartitions:
    """Always depend on a fixed set of upstream partitions, regardless of
    downstream key. Used e.g. for ``step0 gate``: every step depends on
    the root branch's step0.
    """

    keys: frozenset[str]

    def resolve(self, downstream_branch: str) -> frozenset[str]:
        return self.keys


@dataclass(frozen=True)
class ParentOfDownstream:
    """Mirror the downstream branch's variant-tree parent. Optionally
    walks all the way to the root (transitive ancestors).

    * include_self=True  → also include downstream itself (typical for
                            cross-branch merges where the downstream
                            also reads its own prior step).
    * to_root=False      → only the immediate parent.
    * to_root=True       → all ancestors up to the root.
    """

    include_self: bool = True
    to_root: bool = False
    hierarchy: BranchHierarchy = field(default_factory=default_hierarchy)

    def resolve(self, downstream_branch: str) -> frozenset[str]:
        out: set[str] = set()
        if self.include_self:
            out.add(downstream_branch)
        if self.to_root:
            out.update(self.hierarchy.ancestors_of(downstream_branch))
        else:
            p = self.hierarchy.parent_of(downstream_branch)
            if p is not None:
                out.add(p)
        return frozenset(out)


@dataclass(frozen=True)
class RootBranch:
    """Always resolve to the variant-tree root (regardless of downstream).

    Used for the ``setup_gate``: every step must wait on the root
    branch's step0.
    """

    hierarchy: BranchHierarchy = field(default_factory=default_hierarchy)

    def resolve(self, downstream_branch: str) -> frozenset[str]:
        return frozenset(self.hierarchy.roots())


@dataclass(frozen=True)
class UnionOf:
    """Composite: union of multiple rules. Used when two DepRules emit
    edges with the same upstream step; the registry merges their
    partition rules into one UnionOf.
    """

    rules: tuple[PartitionRule, ...]

    def resolve(self, downstream_branch: str) -> frozenset[str]:
        out: set[str] = set()
        for r in self.rules:
            out.update(r.resolve(downstream_branch))
        return frozenset(out)
