"""Lesson 11 — multi-library + UI scaling techniques.

Builds on lesson 10's branched flow, but generates the full
DAG for EACH library programmatically. 2 libraries × 11 assets
each = 22 + 1 root + 1 cross-lib = 24 assets total.

Two scaling techniques demonstrated for UI navigability:
- `key_prefix=[<library>]` — namespaces asset keys by library,
  making the lineage UI group them under expandable folders
- `group_name=f"{library}_{branch}"` — colors assets by library
  + branch, making the visual graph navigable even at 24+ assets

The same techniques scale to 6 libraries × 30 branches = 540+
assets in production.

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
    LIBRARIES,
    LVF_PVTRCS,
    corner_partitions,
    em_partitions,
    lvf_partitions,
)

LESSON_ROOT = Path(__file__).parent.parent
SCRIPTS = LESSON_ROOT / "scripts"
OUTPUT_ROOT = Path("/tmp/dagster-11-flow")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def _digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()[:16]


def _branch_step_path(library: str, branch: str, step: int, pvtrc: str) -> Path:
    """Library-namespaced output: /tmp/dagster-11-flow/<lib>/<branch>/step<N>/<pvtrc>.out"""
    d = OUTPUT_ROOT / library / branch / f"step{step}"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{pvtrc}.out"


# ── shared root (library-agnostic) ──────────────────────────────

@asset(group_name="shared")
def cell_list(context: AssetExecutionContext) -> MaterializeResult:
    output = OUTPUT_ROOT / "cells.json"
    cmd = ["perl", str(SCRIPTS / "perl" / "cell_list.pl"), "--out", str(output)]
    subprocess.run(cmd, check=True)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"path": str(output),
                  "cell_count": len(json.loads(output.read_text()))},
    )


# ── per-library factory ─────────────────────────────────────────

def _build_library_assets(library: str):
    """Generate the 11-asset subgraph for one library.

    Asset keys are e.g. ['svt', 'corner_step1'], etc. Group names
    use 'svt_corner', 'svt_lvf', 'svt_em' for visual grouping.
    """
    assets = []

    def _read_corner_at(pvtrc: str, step: int) -> str:
        p = _branch_step_path(library, "corner", step, pvtrc)
        if not p.exists():
            raise RuntimeError(
                f"corner_step{step} @ {library}/{pvtrc} missing at {p}; "
                f"materialize {library} corner first",
            )
        return p.read_text()

    def _read_branch_prev(branch: str, pvtrc: str, step: int) -> str | None:
        if step <= 1:
            return None
        p = _branch_step_path(library, branch, step - 1, pvtrc)
        if not p.exists():
            raise RuntimeError(
                f"{branch}_step{step - 1} @ {library}/{pvtrc} missing",
            )
        return p.read_text()

    def _make_corner_step(step: int, prev_dep: AssetKey | None):
        @asset(
            key_prefix=[library],
            name=f"corner_step{step}",
            partitions_def=corner_partitions,
            group_name=f"{library}_corner",
            deps=[d for d in [AssetKey("cell_list"), prev_dep]
                  if d is not None],
        )
        def _step_asset(context: AssetExecutionContext) -> MaterializeResult:
            pvtrc = context.partition_key
            prev = (None if step == 1
                    else _read_branch_prev("corner", pvtrc, step))

            out = _branch_step_path(library, "corner", step, pvtrc)
            body = (
                f"library={library} branch=corner step={step} "
                f"pvtrc={pvtrc}\nrole=primary\n"
            )
            if prev:
                body += f"prev_corner_digest={hashlib.sha256(prev.encode()).hexdigest()[:16]}\n"
            out.write_text(body)
            return MaterializeResult(
                data_version=DataVersion(_digest(out)),
                metadata={"library": library, "branch": "corner",
                          "step": step, "pvtrc": pvtrc, "path": str(out)},
            )
        return _step_asset

    corner_step1 = _make_corner_step(1, None)
    corner_step2 = _make_corner_step(2, AssetKey([library, "corner_step1"]))
    corner_step3 = _make_corner_step(3, AssetKey([library, "corner_step2"]))
    assets.extend([corner_step1, corner_step2, corner_step3])

    def _make_secondary_step(branch: str, step: int, partitions_def,
                              prev_dep: AssetKey | None):
        deps = [AssetKey([library, f"corner_step{step}"])]
        if prev_dep is not None:
            deps.append(prev_dep)

        @asset(
            key_prefix=[library],
            name=f"{branch}_step{step}",
            partitions_def=partitions_def,
            group_name=f"{library}_{branch}",
            deps=deps,
        )
        def _step_asset(context: AssetExecutionContext) -> MaterializeResult:
            pvtrc = context.partition_key
            corner_at_step = _read_corner_at(pvtrc, step)
            corner_digest = hashlib.sha256(
                corner_at_step.encode(),
            ).hexdigest()[:16]
            prev = _read_branch_prev(branch, pvtrc, step)
            prev_digest = (
                hashlib.sha256(prev.encode()).hexdigest()[:16]
                if prev else None
            )

            out = _branch_step_path(library, branch, step, pvtrc)
            body = (
                f"library={library} branch={branch} step={step} "
                f"pvtrc={pvtrc}\nrole=secondary\n"
                f"corner_at_step_digest={corner_digest}\n"
            )
            if prev_digest:
                body += f"prev_{branch}_digest={prev_digest}\n"
            out.write_text(body)
            return MaterializeResult(
                data_version=DataVersion(_digest(out)),
                metadata={"library": library, "branch": branch,
                          "step": step, "pvtrc": pvtrc,
                          "corner_at_step_digest": corner_digest},
            )
        return _step_asset

    for branch, partitions_def, pvtrcs in [
        ("lvf", lvf_partitions, LVF_PVTRCS),
        ("em",  em_partitions,  EM_PVTRCS),
    ]:
        s1 = _make_secondary_step(branch, 1, partitions_def, None)
        s2 = _make_secondary_step(
            branch, 2, partitions_def,
            AssetKey([library, f"{branch}_step1"]),
        )
        s3 = _make_secondary_step(
            branch, 3, partitions_def,
            AssetKey([library, f"{branch}_step2"]),
        )
        assets.extend([s1, s2, s3])

    @asset(
        key_prefix=[library],
        name="lib_signoff",
        group_name=f"{library}_signoff",
        deps=[
            AssetKey([library, "corner_step3"]),
            AssetKey([library, "lvf_step3"]),
            AssetKey([library, "em_step3"]),
        ],
    )
    def lib_signoff(context: AssetExecutionContext) -> MaterializeResult:
        sections = []
        for branch, pvtrcs in [
            ("corner", CORNER_PVTRCS),
            ("lvf", LVF_PVTRCS),
            ("em", EM_PVTRCS),
        ]:
            for pvtrc in pvtrcs:
                p = _branch_step_path(library, branch, 3, pvtrc)
                sections.append(f"=== {branch}/{pvtrc} ===\n{p.read_text()}")
        out = OUTPUT_ROOT / library / "signoff.tar"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            f"#! library={library} signoff package\n"
            f"#! sections={len(sections)}\n\n"
            + "\n\n".join(sections),
        )
        return MaterializeResult(
            data_version=DataVersion(_digest(out)),
            metadata={"library": library,
                      "section_count": len(sections),
                      "size_bytes": out.stat().st_size},
        )
    assets.append(lib_signoff)

    return assets


# Build per-library asset families
_per_lib_assets: list = []
for _lib in LIBRARIES:
    _per_lib_assets.extend(_build_library_assets(_lib))


# ── final cross-library signoff ─────────────────────────────────

@asset(
    group_name="shared",
    deps=[AssetKey([lib, "lib_signoff"]) for lib in LIBRARIES],
)
def cross_library_signoff(
    context: AssetExecutionContext,
) -> MaterializeResult:
    parts = []
    for lib in LIBRARIES:
        p = OUTPUT_ROOT / lib / "signoff.tar"
        parts.append(f"=== {lib} ===\n{p.read_text()}")
    out = OUTPUT_ROOT / "cross_library_signoff.tar"
    out.write_text(
        f"#! cross-library signoff\n"
        f"#! libraries={','.join(LIBRARIES)}\n\n"
        + "\n\n".join(parts),
    )
    return MaterializeResult(
        data_version=DataVersion(_digest(out)),
        metadata={"library_count": len(LIBRARIES),
                  "libraries": LIBRARIES,
                  "size_bytes": out.stat().st_size},
    )


defs = Definitions(
    assets=[cell_list, *_per_lib_assets, cross_library_signoff],
)
