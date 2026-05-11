"""Lesson 10 — branched characterization (corner / lvf / em).

11 assets:
  - cell_list                        (root, library-agnostic)
  - corner_step1, _step2, _step3     (per-PVTRC, full set: ff_125/tt_25/ss_m40)
  - lvf_step1, _step2, _step3        (per-PVTRC, subset: tt_25)
  - em_step1, _step2, _step3         (per-PVTRC, subset: ff_125/ss_m40)
  - cross_branch_signoff             (final, fans in over all 3 branches)

Dep pattern (the key insight Brian asked for):
  corner.N depends on corner.(N-1)             — branch is self-contained
  lvf.N    depends on (corner.N, lvf.(N-1))    — needs corner same step + own prev
  em.N     depends on (corner.N, em.(N-1))     — needs corner same step + own prev

Same-PVTRC matching across branches: lvf @ tt_25 reads corner @
tt_25 outputs; em @ ff_125 reads corner @ ff_125 outputs. We use
Style B (filesystem fan-in) to sidestep PartitionMapping
complexity when the per-branch partition sets differ.

Run: dagster dev -m pipelines
Smoke: python -m _smoke
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
    asset,
)

from .partitions import (
    BRANCHES,
    CORNER_PVTRCS,
    EM_PVTRCS,
    LVF_PVTRCS,
    STEPS,
    corner_partitions,
    em_partitions,
    lvf_partitions,
)

LESSON_ROOT = Path(__file__).parent.parent
SCRIPTS = LESSON_ROOT / "scripts"
OUTPUT_ROOT = Path("/tmp/dagster-10-flow")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# Branch step naming convention. Output files live at
# /tmp/dagster-10-flow/<branch>/step<N>/<pvtrc>.out
BRANCH_DIRS = {b: OUTPUT_ROOT / b for b in BRANCHES}
for d in BRANCH_DIRS.values():
    d.mkdir(parents=True, exist_ok=True)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _branch_step_path(branch: str, step: int, pvtrc: str) -> Path:
    """Standard output path for any branch/step/pvtrc."""
    d = BRANCH_DIRS[branch] / f"step{step}"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{pvtrc}.out"


# ── shared root ─────────────────────────────────────────────────

@asset
def cell_list(context: AssetExecutionContext) -> MaterializeResult:
    """Cell list — library- and branch-agnostic root."""
    output = OUTPUT_ROOT / "cells.json"
    cmd = ["perl", str(SCRIPTS / "perl" / "cell_list.pl"), "--out", str(output)]
    subprocess.run(cmd, check=True)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"path": str(output), "cell_count":
                  len(json.loads(output.read_text()))},
    )


# ── helpers used by per-branch step bodies ──────────────────────

def _read_corner_at(pvtrc: str, step: int) -> str:
    """Read corner branch's output for the matching PVTRC at given step.

    This is the cross-branch dep mechanism: lvf and em call this
    to fetch corner's same-PVTRC same-step output.

    Raises if the file isn't there — useful for fail-fast when
    corner hasn't been materialized yet.
    """
    p = _branch_step_path("corner", step, pvtrc)
    if not p.exists():
        raise RuntimeError(
            f"corner_step{step} @ {pvtrc} missing at {p}; "
            f"materialize corner first (lvf/em depend on corner)",
        )
    return p.read_text()


def _read_branch_prev(branch: str, pvtrc: str, current_step: int) -> str | None:
    """Read this branch's own previous step output for the same PVTRC.

    Returns None for step 1 (no previous step exists).
    """
    if current_step <= 1:
        return None
    p = _branch_step_path(branch, current_step - 1, pvtrc)
    if not p.exists():
        raise RuntimeError(
            f"{branch}_step{current_step - 1} @ {pvtrc} missing at "
            f"{p}; materialize previous step first",
        )
    return p.read_text()


# ── corner branch (3 steps) ─────────────────────────────────────
# Self-contained. corner.N deps corner.(N-1) only.

def _make_corner_step(step: int, prev_dep: AssetKey | None):
    @asset(
        name=f"corner_step{step}",
        partitions_def=corner_partitions,
        deps=[d for d in [AssetKey("cell_list"),
                          prev_dep] if d is not None],
    )
    def _step_asset(context: AssetExecutionContext) -> MaterializeResult:
        pvtrc = context.partition_key
        prev_content = (None if step == 1
                        else _read_branch_prev("corner", pvtrc, step))

        out = _branch_step_path("corner", step, pvtrc)
        body = (
            f"branch=corner step={step} pvtrc={pvtrc}\n"
            f"role=primary\n"
        )
        if prev_content:
            prev_digest = hashlib.sha256(prev_content.encode()).hexdigest()[:16]
            body += f"prev_corner_digest={prev_digest}\n"
        out.write_text(body)
        return MaterializeResult(
            data_version=DataVersion(_digest(out)),
            metadata={
                "branch": "corner", "step": step, "pvtrc": pvtrc,
                "path": str(out),
            },
        )
    return _step_asset


corner_step1 = _make_corner_step(1, None)
corner_step2 = _make_corner_step(2, AssetKey("corner_step1"))
corner_step3 = _make_corner_step(3, AssetKey("corner_step2"))


# ── lvf / em branches (3 steps each, same shape) ─────────────────
# Pattern: branch.N deps (corner.N at same PVTRC, branch.(N-1)).

def _make_secondary_step(branch: str, step: int,
                          partitions_def, prev_dep: AssetKey | None):
    """Factory for an `lvf_stepN` or `em_stepN` asset.

    Deps logical:
      step 1: only corner_step1 (no prev within this branch)
      step N>1: corner_step{N} + {branch}_step{N-1}
    """
    deps = [AssetKey(f"corner_step{step}")]
    if prev_dep is not None:
        deps.append(prev_dep)

    @asset(
        name=f"{branch}_step{step}",
        partitions_def=partitions_def,
        deps=deps,
    )
    def _step_asset(context: AssetExecutionContext) -> MaterializeResult:
        pvtrc = context.partition_key

        # Cross-branch dep: read corner's output at SAME step + SAME PVTRC
        corner_at_step = _read_corner_at(pvtrc, step)
        corner_digest = hashlib.sha256(
            corner_at_step.encode(),
        ).hexdigest()[:16]

        # Intra-branch dep: read this branch's previous step output
        prev = _read_branch_prev(branch, pvtrc, step)
        prev_digest = (
            hashlib.sha256(prev.encode()).hexdigest()[:16]
            if prev else None
        )

        out = _branch_step_path(branch, step, pvtrc)
        body = (
            f"branch={branch} step={step} pvtrc={pvtrc}\n"
            f"role=secondary\n"
            f"corner_at_step_digest={corner_digest}\n"
        )
        if prev_digest:
            body += f"prev_{branch}_digest={prev_digest}\n"
        out.write_text(body)

        meta = {
            "branch": branch, "step": step, "pvtrc": pvtrc,
            "path": str(out),
            "corner_at_step_digest": corner_digest,
        }
        if prev_digest:
            meta[f"prev_{branch}_digest"] = prev_digest

        context.log.info(
            f"{branch}_step{step}@{pvtrc}: corner_digest={corner_digest}"
            + (f", prev={prev_digest}" if prev_digest else "")
        )
        return MaterializeResult(
            data_version=DataVersion(_digest(out)),
            metadata=meta,
        )
    return _step_asset


lvf_step1 = _make_secondary_step("lvf", 1, lvf_partitions, None)
lvf_step2 = _make_secondary_step("lvf", 2, lvf_partitions, AssetKey("lvf_step1"))
lvf_step3 = _make_secondary_step("lvf", 3, lvf_partitions, AssetKey("lvf_step2"))

em_step1 = _make_secondary_step("em", 1, em_partitions, None)
em_step2 = _make_secondary_step("em", 2, em_partitions, AssetKey("em_step1"))
em_step3 = _make_secondary_step("em", 3, em_partitions, AssetKey("em_step2"))


# ── final fan-in across all 3 branches ──────────────────────────

@asset(deps=[
    AssetKey("corner_step3"),
    AssetKey("lvf_step3"),
    AssetKey("em_step3"),
])
def cross_branch_signoff(context: AssetExecutionContext) -> MaterializeResult:
    """Final assembly: read each branch's last step outputs (every
    PVTRC of that branch's set) and produce one combined package.
    """
    sections = []
    for branch, pvtrcs in [
        ("corner", CORNER_PVTRCS),
        ("lvf", LVF_PVTRCS),
        ("em", EM_PVTRCS),
    ]:
        for pvtrc in pvtrcs:
            p = _branch_step_path(branch, 3, pvtrc)
            sections.append(f"=== {branch}/{pvtrc} ===\n{p.read_text()}")

    out = OUTPUT_ROOT / "cross_branch_signoff.tar"
    out.write_text(
        f"#! cross-branch signoff package\n"
        f"#! branches={','.join(BRANCHES)}\n"
        f"#! corner_pvtrcs={','.join(CORNER_PVTRCS)}\n"
        f"#! lvf_pvtrcs={','.join(LVF_PVTRCS)}\n"
        f"#! em_pvtrcs={','.join(EM_PVTRCS)}\n\n"
        + "\n\n".join(sections),
    )
    return MaterializeResult(
        data_version=DataVersion(_digest(out)),
        metadata={
            "size_bytes": out.stat().st_size,
            "section_count": len(sections),
            "branches": BRANCHES,
        },
    )


defs = Definitions(
    assets=[
        cell_list,
        corner_step1, corner_step2, corner_step3,
        lvf_step1, lvf_step2, lvf_step3,
        em_step1, em_step2, em_step3,
        cross_branch_signoff,
    ],
)
