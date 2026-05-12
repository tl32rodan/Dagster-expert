"""Variant-tree queries. Pure functions; data loaded from
``config/branches.yaml`` at import time.

Vocabulary (graph-theory aligned, not EDA-specific):

* ``parent_of(b)``   — the immediate variant-tree parent of ``b``;
                       ``None`` if ``b`` is the tree root.
* ``is_root(b)``     — true iff ``b`` has no parent.
* ``ancestors_of(b)``— transitive parents up to (and including) the root.

The specific branch named ``"corner"`` is the tree root in this demo,
but its role is ``root``; tests pin role, not name.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml

_DEFAULT_BRANCHES_YAML = (
    Path(__file__).resolve().parents[2] / "config" / "branches.yaml"
)


@dataclass(frozen=True)
class BranchInfo:
    name: str
    family: str
    parent: Optional[str]


@dataclass(frozen=True)
class BranchHierarchy:
    """In-memory variant tree. Pure data; no Dagster."""

    by_name: dict[str, BranchInfo]

    def all_branches(self) -> tuple[str, ...]:
        return tuple(self.by_name.keys())

    def parent_of(self, branch: str) -> Optional[str]:
        return self.by_name[branch].parent

    def is_root(self, branch: str) -> bool:
        return self.parent_of(branch) is None

    def family_of(self, branch: str) -> str:
        return self.by_name[branch].family

    def roots(self) -> tuple[str, ...]:
        return tuple(b for b in self.all_branches() if self.is_root(b))

    def ancestors_of(self, branch: str) -> tuple[str, ...]:
        out: list[str] = []
        cur = self.parent_of(branch)
        seen: set[str] = set()
        while cur is not None:
            if cur in seen:
                raise ValueError(f"cycle detected at {cur} from {branch}")
            seen.add(cur)
            out.append(cur)
            cur = self.parent_of(cur)
        return tuple(out)

    def descendants_of(self, branch: str) -> tuple[str, ...]:
        out: list[str] = []
        for cand in self.all_branches():
            if branch in self.ancestors_of(cand):
                out.append(cand)
        return tuple(out)


def load(path: Path = _DEFAULT_BRANCHES_YAML) -> BranchHierarchy:
    raw = yaml.safe_load(path.read_text())
    by_name: dict[str, BranchInfo] = {}
    for name, attrs in raw["branches"].items():
        by_name[name] = BranchInfo(
            name=name,
            family=attrs["family"],
            parent=attrs.get("parent"),
        )
    # Validate: every non-null parent must exist.
    for info in by_name.values():
        if info.parent is not None and info.parent not in by_name:
            raise ValueError(
                f"branch {info.name!r} parent {info.parent!r} not defined",
            )
    return BranchHierarchy(by_name=by_name)


@lru_cache(maxsize=1)
def default() -> BranchHierarchy:
    """Singleton loaded from the demo's config/branches.yaml."""
    return load()
