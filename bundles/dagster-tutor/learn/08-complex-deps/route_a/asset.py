"""lab8 route_a: concrete assets for sparse (corner, em, ht) matrix.

One @asset per valid (corner, em, ht) tuple. Each step6_<corner>
fans in over all step5__<corner>__* of that corner plus
step0__<corner>.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict

from dagster import (
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)

CORNERS = ["ff", "tt", "ss", "sf"]

# Sparse: only these (corner, em, ht) tuples are valid
VALID_COMBOS = [
    ("ff", "em_lo", "ht_low"),
    ("ff", "em_hi", "ht_high"),
    ("tt", "em_mid", "ht_mid"),
    ("ss", "em_lo", "ht_low"),
    ("ss", "em_mid", "ht_mid"),
    ("ss", "em_hi", "ht_high"),
    ("sf", "em_mid", "ht_mid"),
]


def _payload(label: str) -> MaterializeResult:
    data = label.encode()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(data).hexdigest()[:16]),
        metadata={"label": label, "size_bytes": len(data)},
    )


# step0 — corner-only
def _make_step0(corner: str):
    @asset(name=f"step0__{corner}")
    def step0() -> MaterializeResult:
        return _payload(f"step0:{corner}")
    return step0


# step5 — concrete asset per valid combo
def _make_step5(corner: str, em: str, ht: str):
    @asset(
        name=f"step5__{corner}__{em}__{ht}",
        deps=[AssetKey(f"step0__{corner}")],
    )
    def step5() -> MaterializeResult:
        return _payload(f"step5:{corner}:{em}:{ht}")
    return step5


# step6 — fan-in over all step5 of one corner + step0 of that corner
def _make_step6(corner: str, valid_for_corner: list[tuple[str, str, str]]):
    upstream_keys = [AssetKey(f"step0__{corner}")] + [
        AssetKey(f"step5__{corner}__{em}__{ht}")
        for (_, em, ht) in valid_for_corner
    ]

    @asset(
        name=f"step6__{corner}",
        deps=upstream_keys,
    )
    def step6() -> MaterializeResult:
        return _payload(f"step6:{corner}")
    return step6


# Build the full asset list
by_corner: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
for combo in VALID_COMBOS:
    by_corner[combo[0]].append(combo)

assets = []
for corner in CORNERS:
    if corner not in by_corner:
        continue
    assets.append(_make_step0(corner))
    for combo in by_corner[corner]:
        assets.append(_make_step5(*combo))
    assets.append(_make_step6(corner, by_corner[corner]))


defs = Definitions(assets=assets)
