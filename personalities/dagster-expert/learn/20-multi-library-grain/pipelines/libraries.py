"""100 library names for the production-scale demo.

A real shop swaps this list for actual library names (Vt class × process
× voltage × cell-height combos). For lesson 20 we use a simple naming
scheme that survives air-gap and stays sortable in the Dagster UI.

Naming convention (mimics TSMC AP shop style without leaking specifics):

    {vt}_{process}_{height}_{nominal_v}

    vt        ∈ {svt, lvt, ulvt, hvt, elvt, ulvthp}             (6 Vt classes)
    process   ∈ {p1, p2}                                         (2 process splits)
    height    ∈ {h6, h7p5, h9}                                   (3 cell heights)
    nominal_v ∈ {075, 080, 090}                                  (3 nominal voltages)

    6 × 2 × 3 × 3 = 108 combos — we take the first 100 by deterministic
    enumeration so the list is stable across runs.

The lesson does not care about the names being EDA-realistic; it cares
that there are 100 of them and each maps to one Dagster ``group_name``.
"""
from __future__ import annotations


_VTS = ("svt", "lvt", "ulvt", "hvt", "elvt", "ulvthp")
_PROCESSES = ("p1", "p2")
_HEIGHTS = ("h6", "h7p5", "h9")
_VOLTAGES = ("075", "080", "090")


def _enumerate() -> tuple[str, ...]:
    out: list[str] = []
    # Outer loop on vt, then process, then height, then voltage — gives
    # a stable lexicographic-ish order grouped by Vt class.
    for vt in _VTS:
        for proc in _PROCESSES:
            for h in _HEIGHTS:
                for v in _VOLTAGES:
                    out.append(f"{vt}_{proc}_{h}_{v}")
    return tuple(out[:100])


LIBRARIES: tuple[str, ...] = _enumerate()


assert len(LIBRARIES) == 100, f"expected 100 libraries, got {len(LIBRARIES)}"
assert len(set(LIBRARIES)) == 100, "library names must be unique"
