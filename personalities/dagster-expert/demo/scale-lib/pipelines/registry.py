"""Layer 2 — single source of truth for dep facts.

Edit *this file* to change the demo's dep behavior. Everything below
this layer (factory, definitions) consumes DEPS via ``edges_for``.

Tests in ``tests/test_registry.py`` pin the resolved edge set per
(library, step). Add a test there for any non-trivial new rule.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Tuple

from .rules.kit_rln import KitRlnRule
from .rules.kit_step6 import KitStep6Rule
from .rules.parent_mirror import ParentMirrorRule
from .rules.setup_gate import SetupGateRule
from .rules.step7_follow import Step7FollowRule
from .rules.step_chain import StepChainRule
from .spec.dep_edge import DepEdge
from .spec.dep_rule import DepRule
from .spec.partition_rule import PartitionRule, UnionOf


@dataclass(frozen=True)
class DepRegistry:
    rules: Tuple[DepRule, ...]

    def edges_for(self, library: str, step: str) -> list[DepEdge]:
        """Returns merged edges for (library, step).

        Multiple rules emitting edges to the same upstream
        ``(library, step)`` are merged into a single edge whose
        PartitionRule is a ``UnionOf`` of the contributors. This keeps
        the @asset's ``ins=`` dict free of duplicate keys.
        """
        # Bucket by (upstream_library, upstream_step).
        buckets: dict[Tuple[str | None, str], list[PartitionRule]] = defaultdict(list)
        for rule in self.rules:
            for edge in rule.emit_edges(library, step):
                buckets[(edge.upstream_library, edge.upstream_step)].append(
                    edge.partition_rule,
                )

        merged: list[DepEdge] = []
        for (up_lib, up_step), rules in buckets.items():
            pr = rules[0] if len(rules) == 1 else UnionOf(tuple(rules))
            merged.append(
                DepEdge(
                    upstream_step=up_step,
                    upstream_library=up_lib,
                    partition_rule=pr,
                ),
            )
        return merged

    def with_rules(self, extra: Iterable[DepRule]) -> "DepRegistry":
        return DepRegistry(rules=tuple(self.rules) + tuple(extra))


def _default_rules() -> Tuple[DepRule, ...]:
    """The rule list that ships with the demo.

    Override ``CORNER_MERGE_STEPS`` by re-instantiating ParentMirrorRule
    with a different set, or by passing additional rules to
    ``DepRegistry`` via ``with_rules``.
    """
    return (
        StepChainRule(),
        ParentMirrorRule(applies_to=frozenset({"step5"})),
        SetupGateRule(),
        Step7FollowRule(),
        KitStep6Rule(rln_exempt=True),
        KitRlnRule(),
    )


DEPS = DepRegistry(rules=_default_rules())
