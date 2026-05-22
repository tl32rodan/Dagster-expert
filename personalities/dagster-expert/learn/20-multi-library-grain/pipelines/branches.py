"""46-branch variant tree for AP characterization.

Pure data + small helpers. No Dagster imports — the partition-mapping
layer (``partition_mappings.py``) consumes this to pre-compute
StaticPartitionMapping rows.

Tree shape (immediate parents only; ``corner`` is the root):

  corner ─┬─ em
          ├─ ht
          ├─ lvf ─── lvf_ht
          ├─ mpwda ─┬─ mpwda_aged ── mpwda_aged_lvf
          │        └─ mpwda_lvf
          └─ tmsf_self ─┬─ tmsf_self_ht
                        ├─ tmsf_self_lvf ── tmsf_self_lvf_ht
                        ├─ tmsf_lde1  ─── tmsf_lde1_ht
                        ├─ tmsf_lde2  ─── tmsf_lde2_ht
                        ├─ ...                      (lde1..lde10 each have _ht)
                        ├─ tmsf_lde10 ── tmsf_lde10_ht
                        └─ tmsf_lde11..tmsf_lde23   (no _ht)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ── Source of truth ─────────────────────────────────────────────────
#
# (branch_name, family, parent_or_None). Order matters only for stable
# enumeration in tests / UI.

_BRANCH_ROWS: tuple[tuple[str, str, Optional[str]], ...] = (
    # family: main
    ("corner",            "main",        None),

    # family: standard
    ("em",                "standard",    "corner"),
    ("ht",                "standard",    "corner"),
    ("lvf",               "standard",    "corner"),
    ("lvf_ht",            "standard",    "lvf"),

    # family: aged
    ("mpwda",             "aged",        "corner"),
    ("mpwda_aged",        "aged",        "mpwda"),
    ("mpwda_lvf",         "aged",        "mpwda"),
    ("mpwda_aged_lvf",    "aged",        "mpwda_aged"),

    # family: tmsf_corner
    ("tmsf_self",         "tmsf_corner", "corner"),
    ("tmsf_self_ht",      "tmsf_corner", "tmsf_self"),
    ("tmsf_self_lvf",     "tmsf_corner", "tmsf_self"),
    ("tmsf_self_lvf_ht",  "tmsf_corner", "tmsf_self_lvf"),

    # family: tmsf_base (23 lde variants)
    *((f"tmsf_lde{i}", "tmsf_base", "tmsf_self") for i in range(1, 24)),

    # family: tmsf_ht (10 lde_ht variants; each mirrors the matching lde)
    *((f"tmsf_lde{i}_ht", "tmsf_ht", f"tmsf_lde{i}") for i in range(1, 11)),
)


@dataclass(frozen=True)
class BranchSpec:
    name: str
    family: str
    parent: Optional[str]


BRANCHES: tuple[BranchSpec, ...] = tuple(
    BranchSpec(name=n, family=f, parent=p) for n, f, p in _BRANCH_ROWS
)

_BY_NAME: dict[str, BranchSpec] = {b.name: b for b in BRANCHES}


# ── Graph-theory helpers (terminology: graph theory, not EDA) ──────


def all_branches() -> tuple[str, ...]:
    return tuple(b.name for b in BRANCHES)


def parent_of(branch: str) -> Optional[str]:
    return _BY_NAME[branch].parent


def is_root(branch: str) -> bool:
    return _BY_NAME[branch].parent is None


def roots() -> frozenset[str]:
    return frozenset(b.name for b in BRANCHES if b.parent is None)


def ancestors_of(branch: str) -> frozenset[str]:
    """All ancestors up to and including the root (excludes branch itself)."""
    out: set[str] = set()
    cur = parent_of(branch)
    while cur is not None:
        out.add(cur)
        cur = parent_of(cur)
    return frozenset(out)


def branches_by_family() -> dict[str, tuple[str, ...]]:
    by: dict[str, list[str]] = {}
    for b in BRANCHES:
        by.setdefault(b.family, []).append(b.name)
    return {fam: tuple(names) for fam, names in by.items()}
