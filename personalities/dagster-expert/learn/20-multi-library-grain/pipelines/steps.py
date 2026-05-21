"""21-step taxonomy for AP characterization (the grain tier).

Pure data + helpers. Each step is either:

- ``SETUP_ROOT_ONLY``  — runs only on the ``corner`` branch (root).
- ``EXTRACTION``       — runs on all 46 branches.
- ``CHAR``             — runs on all 46 branches; step2-6 are a linear chain.
- ``KIT_ROOT_ONLY``    — runs only on the ``corner`` branch.

The chain (step2 → step3 → step4 → step5 → step6) defines intra-step
``SameBranch`` dependency (identity partition mapping). Step5 also
mirrors its variant-tree parent at the same step level (parent-mirror
rule).

Note: ``step7`` is an extraction step that depends on step6 finishing,
modeling a post-char extraction. Kits all gate on step6 of the root
branch (``KitStep6Rule`` in factory.py).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class StepKind(str, Enum):
    SETUP_ROOT_ONLY = "setup_root_only"   # step0, auto_download
    EXTRACTION = "extraction"             # phantom, BEpreQ, step1, step7
    CHAR = "char"                         # step2..step6, FunKits
    KIT_ROOT_ONLY = "kit_root_only"       # rln, trf, cdk, pgv, apl, spm, mpwda_kit, mtbf, meta


@dataclass(frozen=True)
class StepSpec:
    name: str
    kind: StepKind
    # The CHAR linear-chain index (step2..step6). None = not in chain.
    chain_index: Optional[int] = None


STEPS: tuple[StepSpec, ...] = (
    # SETUP (root branch only)
    StepSpec("step0",         StepKind.SETUP_ROOT_ONLY),
    StepSpec("auto_download", StepKind.SETUP_ROOT_ONLY),

    # EXTRACTION (all branches)
    StepSpec("phantom",       StepKind.EXTRACTION),
    StepSpec("BEpreQ",        StepKind.EXTRACTION),
    StepSpec("step1",         StepKind.EXTRACTION),
    StepSpec("step7",         StepKind.EXTRACTION),

    # CHAR (all branches; step2..step6 chained, FunKits unchained)
    StepSpec("step2",         StepKind.CHAR, chain_index=2),
    StepSpec("step3",         StepKind.CHAR, chain_index=3),
    StepSpec("step4",         StepKind.CHAR, chain_index=4),
    StepSpec("step5",         StepKind.CHAR, chain_index=5),
    StepSpec("step6",         StepKind.CHAR, chain_index=6),
    StepSpec("FunKits",       StepKind.CHAR),

    # KITS (root branch only)
    StepSpec("rln",           StepKind.KIT_ROOT_ONLY),
    StepSpec("trf",           StepKind.KIT_ROOT_ONLY),
    StepSpec("cdk",           StepKind.KIT_ROOT_ONLY),
    StepSpec("pgv",           StepKind.KIT_ROOT_ONLY),
    StepSpec("apl",           StepKind.KIT_ROOT_ONLY),
    StepSpec("spm",           StepKind.KIT_ROOT_ONLY),
    StepSpec("mpwda_kit",     StepKind.KIT_ROOT_ONLY),
    StepSpec("mtbf",          StepKind.KIT_ROOT_ONLY),
    StepSpec("meta",          StepKind.KIT_ROOT_ONLY),
)


_BY_NAME: dict[str, StepSpec] = {s.name: s for s in STEPS}


def step(name: str) -> StepSpec:
    return _BY_NAME[name]


def step_names() -> tuple[str, ...]:
    return tuple(s.name for s in STEPS)


def chain() -> tuple[str, ...]:
    """The CHAR linear chain in ascending order (step2..step6)."""
    chained = sorted(
        (s for s in STEPS if s.chain_index is not None),
        key=lambda s: s.chain_index,  # type: ignore[arg-type]
    )
    return tuple(s.name for s in chained)


def prev_in_chain(name: str) -> Optional[str]:
    c = chain()
    if name not in c:
        return None
    i = c.index(name)
    return c[i - 1] if i > 0 else None


def kits() -> tuple[str, ...]:
    return tuple(s.name for s in STEPS if s.kind is StepKind.KIT_ROOT_ONLY)


def setup_steps() -> tuple[str, ...]:
    return tuple(s.name for s in STEPS if s.kind is StepKind.SETUP_ROOT_ONLY)


def is_root_only(name: str) -> bool:
    k = _BY_NAME[name].kind
    return k in (StepKind.SETUP_ROOT_ONLY, StepKind.KIT_ROOT_ONLY)


# Default parent-mirror step set: only step5 needs it. Lifted from
# scale-lib's ParentMirrorRule(applies_to={"step5"}). Override by
# passing a different frozenset to ``build_assets`` in definitions.py.
DEFAULT_PARENT_MIRROR_STEPS: frozenset[str] = frozenset({"step5"})
