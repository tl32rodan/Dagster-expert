"""Lesson 12 multi_location — library 'svt'.

One code location per library. Cardinality per location is
bounded — at production scale, 100 such code locations each load
just their own subset.

This file is intentionally identical-in-shape across libraries
(svt / lvt / ulvt / ...). In real production it'd be programmatically
generated from a template, NOT hand-edited per library.
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)

LIBRARY = "svt"
BRANCHES = ["corner", "lvf", "em"]
STEP_TYPES = ["lpe", "char", "signoff"]
PVTRCS = ["ff_125", "tt_25", "ss_m40"]

BR_KEYS = BRANCHES
br_pvtrc = MultiPartitionsDefinition({
    "branch": StaticPartitionsDefinition(BR_KEYS),
    "pvtrc":  StaticPartitionsDefinition(PVTRCS),
})

OUT = Path("/tmp/dagster-12-multi-loc") / LIBRARY
OUT.mkdir(parents=True, exist_ok=True)


def _digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()[:16]


def _step_path(step: str, branch: str, pvtrc: str) -> Path:
    d = OUT / step
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{branch}__{pvtrc}.out"


def _make_step(step_type: str, prev_step: str | None):
    deps = []
    if prev_step is not None:
        deps.append(AssetKey([LIBRARY, prev_step]))

    @asset(
        key_prefix=[LIBRARY],
        name=step_type,
        partitions_def=br_pvtrc,
        group_name=LIBRARY,
        deps=deps,
    )
    def _a(context: AssetExecutionContext) -> MaterializeResult:
        keys = context.partition_key.keys_by_dimension
        branch, pvtrc = keys["branch"], keys["pvtrc"]

        prev_digest = None
        if prev_step is not None:
            prev_path = _step_path(prev_step, branch, pvtrc)
            if not prev_path.exists():
                raise RuntimeError(
                    f"{LIBRARY}/{step_type}: missing {prev_path}"
                )
            prev_digest = hashlib.sha256(prev_path.read_bytes()).hexdigest()[:16]

        out = _step_path(step_type, branch, pvtrc)
        body = f"library={LIBRARY} step={step_type} branch={branch} pvtrc={pvtrc}\n"
        if prev_digest:
            body += f"prev_digest={prev_digest}\n"
        out.write_text(body)
        return MaterializeResult(
            data_version=DataVersion(_digest(out)),
            metadata={"library": LIBRARY, "step": step_type,
                      "branch": branch, "pvtrc": pvtrc},
        )
    return _a


_assets = []
_prev = None
for st in STEP_TYPES:
    _assets.append(_make_step(st, _prev))
    _prev = st


defs = Definitions(assets=_assets)
