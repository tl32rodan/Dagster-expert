"""Step inventory: which steps exist and what kind (which partition shape
they take, which branches they apply to, which runner runs them).

Pure data — used by the factory to pick the right partitions_def and by
the rules to know step ordering.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class StepKind(str, Enum):
    SETUP_ROOT_ONLY = "setup_root_only"     # step0, auto_download (root branch only)
    EXTRACTION = "extraction"               # all branches
    CHAR = "char"                           # all branches
    KIT_ROOT_ONLY = "kit_root_only"         # kits (root branch only)


class Runner(str, Enum):
    PERL = "perl"
    PYTHON_PIPES = "python_pipes"


@dataclass(frozen=True)
class StepSpec:
    name: str
    kind: StepKind
    runner: Runner
    # The intra-step chain order (step2 -> step3 -> step4 etc.) is given
    # by `chain_index`. None means the step is not in the linear chain
    # (e.g. setup, kits, FunKits, phantom, BEpreQ, step7).
    chain_index: Optional[int] = None


# Single source of truth for what steps exist. Ordered for readability.
STEPS: tuple[StepSpec, ...] = (
    # SETUP — root branch only
    StepSpec("step0",          StepKind.SETUP_ROOT_ONLY, Runner.PERL),
    StepSpec("auto_download",  StepKind.SETUP_ROOT_ONLY, Runner.PERL),

    # EXTRACTION — all branches
    StepSpec("phantom",        StepKind.EXTRACTION,      Runner.PERL),
    StepSpec("BEpreQ",         StepKind.EXTRACTION,      Runner.PERL),
    StepSpec("step1",          StepKind.EXTRACTION,      Runner.PERL),
    StepSpec("step7",          StepKind.EXTRACTION,      Runner.PERL),

    # CHAR — all branches; step2..6 linear chain
    StepSpec("step2",          StepKind.CHAR,            Runner.PYTHON_PIPES, chain_index=2),
    StepSpec("step3",          StepKind.CHAR,            Runner.PYTHON_PIPES, chain_index=3),
    StepSpec("step4",          StepKind.CHAR,            Runner.PYTHON_PIPES, chain_index=4),
    StepSpec("step5",          StepKind.CHAR,            Runner.PYTHON_PIPES, chain_index=5),
    StepSpec("step6",          StepKind.CHAR,            Runner.PERL,         chain_index=6),
    StepSpec("FunKits",        StepKind.CHAR,            Runner.PERL),

    # KITS — root branch only
    StepSpec("rln",            StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("trf",            StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("cdk",            StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("pgv",            StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("apl",            StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("spm",            StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("mpwda_kit",      StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("mtbf",           StepKind.KIT_ROOT_ONLY,   Runner.PERL),
    StepSpec("meta",           StepKind.KIT_ROOT_ONLY,   Runner.PERL),
)


# Index helpers (pure, side-effect-free).

_BY_NAME = {s.name: s for s in STEPS}


def step(name: str) -> StepSpec:
    return _BY_NAME[name]


def step_names() -> tuple[str, ...]:
    return tuple(s.name for s in STEPS)


def chain() -> tuple[str, ...]:
    """The CHAR linear chain in ascending order."""
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
    k = step(name).kind
    return k in (StepKind.SETUP_ROOT_ONLY, StepKind.KIT_ROOT_ONLY)
