"""Lesson 12 — high-cardinality (lesson 11 style) — baseline.

Same logical flow as the compact version, but one @asset per
(library, branch, step_type) tuple. This is what lesson 11
ships at small scale.

At Brian's production scale (6 × 50 × 15), this shape produces
4500 @asset declarations, which trips SQLite's
SQLITE_MAX_VARIABLE_NUMBER (999 / 32766) and bloats the
instance DB. See the compact/ folder for the recommended fix.

This file demos at 2 libs × 4 branches × 5 step-types = 40
@assets for direct side-by-side comparison.
"""

import hashlib
import json
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)

OUTPUT_ROOT = Path("/tmp/dagster-12-high-cardinality")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

LIBRARIES = ["svt", "lvt"]
BRANCHES = ["corner", "lvf", "em", "noise"]
STEP_TYPES = ["lpe", "process_model", "char", "aggregate", "signoff"]
PVTRCS = ["ff_125", "tt_25", "ss_m40"]

pvtrc_partitions = StaticPartitionsDefinition(PVTRCS)


def _digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()[:16]


def _path(library: str, branch: str, step: str, pvtrc: str) -> Path:
    d = OUTPUT_ROOT / library / branch / step
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{pvtrc}.out"


@asset(group_name="shared")
def cell_list(context: AssetExecutionContext) -> MaterializeResult:
    out = OUTPUT_ROOT / "cells.json"
    out.write_text(json.dumps(["INV", "BUF", "NAND2"]))
    return MaterializeResult(
        data_version=DataVersion(_digest(out)),
        metadata={"cell_count": 3, "path": str(out)},
    )


def _make_lbsf_asset(library: str, branch: str, step: str,
                     step_index: int):
    """One @asset per (library, branch, step) tuple."""
    prev_dep = None
    if step_index > 0:
        prev_step = STEP_TYPES[step_index - 1]
        prev_dep = AssetKey([library, branch, prev_step])
    deps = [AssetKey("cell_list")]
    if prev_dep is not None:
        deps.append(prev_dep)

    @asset(
        key_prefix=[library, branch],
        name=step,
        partitions_def=pvtrc_partitions,
        group_name=f"{library}_{branch}",
        deps=deps,
    )
    def _a(context: AssetExecutionContext) -> MaterializeResult:
        pvtrc = context.partition_key
        if prev_dep is not None:
            prev_path = _path(library, branch, STEP_TYPES[step_index - 1],
                              pvtrc)
            if not prev_path.exists():
                raise RuntimeError(
                    f"{library}/{branch}/{step}: prev missing at {prev_path}"
                )
            prev_digest = hashlib.sha256(
                prev_path.read_bytes()
            ).hexdigest()[:16]
        else:
            prev_digest = None

        out = _path(library, branch, step, pvtrc)
        body = (
            f"library={library} branch={branch} step={step} "
            f"pvtrc={pvtrc}\n"
        )
        if prev_digest:
            body += f"prev_digest={prev_digest}\n"
        out.write_text(body)
        return MaterializeResult(
            data_version=DataVersion(_digest(out)),
            metadata={"library": library, "branch": branch,
                      "step": step, "pvtrc": pvtrc},
        )
    return _a


_all_assets = [cell_list]
for lib in LIBRARIES:
    for br in BRANCHES:
        for i, st in enumerate(STEP_TYPES):
            _all_assets.append(_make_lbsf_asset(lib, br, st, i))


defs = Definitions(assets=_all_assets)
