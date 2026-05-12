"""Lesson 12 — compact (multi-partition) form.

Same logical flow as lesson 11, but cardinality reshaped:
- N libraries × M branches × K step-types
- Old (lesson 11):  N×M×K distinct @asset declarations
- New (this file):  K distinct @asset declarations, each with
                    N×M (library_branch composite) partitions

Asset count goes from N×M×K → K. For Brian's production scale:
  6 libs × 50 branches × 15 step-types = 4500 @asset declarations (old)
                                      = 15  @asset declarations (new)

300× fewer asset rows in the Dagster instance DB. The same data
lives in partition rows instead — but Dagster's bulk queries
scale by asset count, not partition count, so SQLite's
?-placeholder ceiling and PG's load both improve.

This file demos at smaller scale: 2 libs × 4 branches × 5 step-types
= 40 (old) vs 5 (new). Smoke time stays under 30s.
"""

import hashlib
import json
import subprocess
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

LESSON_ROOT = Path(__file__).parent.parent
OUTPUT_ROOT = Path("/tmp/dagster-12-compact")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# ── shape parameters ──────────────────────────────────────────
LIBRARIES = ["svt", "lvt"]                              # 2
BRANCHES = ["corner", "lvf", "em", "noise"]             # 4
STEP_TYPES = ["lpe", "process_model", "char",
              "aggregate", "signoff"]                    # 5
PVTRCS = ["ff_125", "tt_25", "ss_m40"]                  # 3

# Composite partition keys — 2D MultiPartitions (1.13.3 limit)
# dim 1: lib_branch  = library__branch  (N×M values)
# dim 2: pvtrc       (P values)
LIB_BRANCH_KEYS = [f"{lib}__{br}" for lib in LIBRARIES for br in BRANCHES]

lib_branch_pvtrc = MultiPartitionsDefinition({
    "lib_branch": StaticPartitionsDefinition(LIB_BRANCH_KEYS),
    "pvtrc":      StaticPartitionsDefinition(PVTRCS),
})


def _digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()[:16]


def _split_lib_branch(key: str) -> tuple[str, str]:
    """`svt__corner` → `("svt", "corner")`."""
    return tuple(key.split("__", 1))  # type: ignore[return-value]


def _step_dir(step_type: str) -> Path:
    d = OUTPUT_ROOT / step_type
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── shared root: cell_list (per library, but flat) ────────────

@asset(group_name="shared")
def cell_list(context: AssetExecutionContext) -> MaterializeResult:
    """Just emit the cell list. Library-agnostic — same cells in
    every library (different transistor flavors)."""
    output = OUTPUT_ROOT / "cells.json"
    output.write_text(json.dumps(["INV", "BUF", "NAND2"]))
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"cell_count": 3, "path": str(output)},
    )


# ── factory: one asset per step type ──────────────────────────

def _make_step_asset(step_type: str, prev_step: str | None):
    """One @asset per step_type. Partitioned by (lib_branch × pvtrc).
    Each materialization is one (library, branch, pvtrc) combo.
    """
    deps = [AssetKey("cell_list")]
    if prev_step is not None:
        deps.append(AssetKey(prev_step))

    @asset(
        name=step_type,
        partitions_def=lib_branch_pvtrc,
        group_name="flow",
        deps=deps,
    )
    def _step(context: AssetExecutionContext) -> MaterializeResult:
        keys = context.partition_key.keys_by_dimension
        lib_branch = keys["lib_branch"]
        pvtrc = keys["pvtrc"]
        library, branch = _split_lib_branch(lib_branch)

        # Read previous step's output for THIS partition (Style B fan-in)
        if prev_step is not None:
            prev_path = (_step_dir(prev_step)
                         / f"{library}__{branch}__{pvtrc}.out")
            if not prev_path.exists():
                raise RuntimeError(
                    f"{step_type}: prev {prev_step} @ {library}/{branch}/"
                    f"{pvtrc} missing at {prev_path}"
                )
            prev_digest = hashlib.sha256(
                prev_path.read_bytes()
            ).hexdigest()[:16]
        else:
            prev_digest = None

        out = _step_dir(step_type) / f"{library}__{branch}__{pvtrc}.out"
        body = (
            f"step={step_type}\n"
            f"library={library} branch={branch} pvtrc={pvtrc}\n"
        )
        if prev_digest:
            body += f"prev_{prev_step}_digest={prev_digest}\n"
        out.write_text(body)

        return MaterializeResult(
            data_version=DataVersion(_digest(out)),
            metadata={
                "step_type": step_type,
                "library": library,
                "branch": branch,
                "pvtrc": pvtrc,
            },
        )
    return _step


# Build the 5 step-type assets sequentially (lpe → process_model → ... → signoff)
_step_assets = []
_prev = None
for st in STEP_TYPES:
    _step_assets.append(_make_step_asset(st, _prev))
    _prev = st


defs = Definitions(assets=[cell_list, *_step_assets])
