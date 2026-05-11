"""Lesson 09 — real AP characterization flow.

7 assets. Mixed Perl + Python + TCL. Demonstrates:
1. Loose subprocess wrapping (Perl scripts that don't know about Dagster)
2. Pipes integration (Python+TCL via dagster_pipes)
3. Fine-grain partition split (LPE per-PVT-per-cell, char per-PVT)
4. Cross-partition fan-in via filesystem (Style B)
5. Incremental rerun via per-partition checkpoint files

Run:
    dagster dev -m pipelines

CLI materialize all (slow — ~5 min):
    dagster asset materialize -m pipelines --select '*'

CLI materialize one PVT only:
    dagster asset materialize -m pipelines --select liberate_run \\
        --partition 'corner=ff|volt_temp=0p9v__25c'
"""

import hashlib
import json
import os
import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
)

from .partitions import (
    CELLS,
    CORNERS,
    VOLT_TEMPS,
    corner_partitions,
    lpe_partitions,
    pvt_partitions,
    split_vt_cell,
)

# Paths -----------------------------------------------------------

LESSON_ROOT = Path(__file__).parent.parent              # learn/09-real-flow/
SCRIPTS = LESSON_ROOT / "scripts"
OUTPUT_ROOT = Path("/tmp/dagster-09-flow")

# Subdirs (created on first run)
DIR_CELL_LIST = OUTPUT_ROOT / "cell_list"
DIR_LPE = OUTPUT_ROOT / "lpe"
DIR_CORNER_SETUP = OUTPUT_ROOT / "corner_setup"
DIR_NETLIST = OUTPUT_ROOT / "netlist_gen"
DIR_LIBERATE = OUTPUT_ROOT / "liberate"
DIR_AGGREGATE = OUTPUT_ROOT / "liberty_aggregate"
DIR_SIGNOFF = OUTPUT_ROOT / "signoff"

for d in [DIR_CELL_LIST, DIR_LPE, DIR_CORNER_SETUP, DIR_NETLIST,
          DIR_LIBERATE, DIR_AGGREGATE, DIR_SIGNOFF]:
    d.mkdir(parents=True, exist_ok=True)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _digest_files(paths: list[Path]) -> str:
    """Stable hash over a sorted list of file contents."""
    h = hashlib.sha256()
    for p in sorted(paths, key=lambda x: x.name):
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


# Step 0 — Perl: cell_list ----------------------------------------

@asset
def cell_list(context: AssetExecutionContext) -> MaterializeResult:
    """Root: defines the standard-cell list to characterize.

    Implementation: Perl. Plain subprocess (Perl doesn't know about
    Dagster). Output file is the truth; we hash it for data_version.
    """
    output = DIR_CELL_LIST / "cells.json"
    cmd = ["perl", str(SCRIPTS / "perl" / "cell_list.pl"), "--out", str(output)]
    context.log.info(f"running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={
            "path": str(output),
            "cell_count": len(json.loads(output.read_text())),
        },
    )


# Step 1 — Perl: lpe (per corner × vt × cell) ---------------------

@asset(partitions_def=lpe_partitions, deps=[AssetKey("cell_list")])
def lpe(context: AssetExecutionContext) -> MaterializeResult:
    """LPE (Layout Parasitic Extraction) per (corner, vt, cell).

    Each partition produces a tiny .spef. Demonstrates the
    finest-grained partition: 3 × 24 = 72 partitions.
    """
    keys = context.partition_key.keys_by_dimension
    corner = keys["corner"]
    vt, cell = split_vt_cell(keys["vt_cell"])

    output = DIR_LPE / f"{corner}__{vt}__{cell}.spef"
    cmd = [
        "perl", str(SCRIPTS / "perl" / "lpe.pl"),
        "--corner", corner, "--vt", vt, "--cell", cell,
        "--cell-list", str(DIR_CELL_LIST / "cells.json"),
        "--out", str(output),
    ]
    context.log.info(f"LPE: {corner}/{vt}/{cell}")
    subprocess.run(cmd, check=True)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"path": str(output), "corner": corner, "vt": vt, "cell": cell},
    )


# Step 2 — Python: corner_setup -----------------------------------

@asset(partitions_def=corner_partitions, deps=[AssetKey("lpe")])
def corner_setup(context: AssetExecutionContext) -> MaterializeResult:
    """Per-corner setup: gather all this corner's LPE outputs.

    Cross-partition fan-in via filesystem glob (Style B). No
    PartitionMapping needed; we just read what's on disk.
    """
    corner = context.partition_key
    spef_files = sorted(DIR_LPE.glob(f"{corner}__*.spef"))
    expected = len(VOLT_TEMPS) * len(CELLS)  # 24
    if len(spef_files) < expected:
        raise RuntimeError(
            f"corner_setup({corner}): expected {expected} LPE files, "
            f"got {len(spef_files)}",
        )
    output = DIR_CORNER_SETUP / f"{corner}.setup"
    output.write_text(
        f"corner={corner}\n"
        f"spef_count={len(spef_files)}\n"
        f"spef_digest={_digest_files(spef_files)}\n",
    )
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"corner": corner, "spef_count": len(spef_files)},
    )


# Step 3 — Python: netlist_gen ------------------------------------

@asset(partitions_def=corner_partitions, deps=[AssetKey("corner_setup")])
def netlist_gen(context: AssetExecutionContext) -> MaterializeResult:
    """Per-corner netlist generation."""
    corner = context.partition_key
    setup = DIR_CORNER_SETUP / f"{corner}.setup"
    output = DIR_NETLIST / f"{corner}.netlist"
    output.write_text(
        f"# netlist for {corner}\n"
        f"# derived from {setup}\n"
        f"setup_digest={_digest(setup)}\n",
    )
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"corner": corner, "path": str(output)},
    )


# Step 4 — Python+TCL via Pipes: liberate_run ---------------------

@asset(partitions_def=pvt_partitions, deps=[AssetKey("netlist_gen")])
def liberate_run(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    """The characterization step. Per (corner, vt) — 18 partitions.

    Invokes a Python wrapper (which exec's a TCL mock) via Dagster
    Pipes. Demonstrates:
    - Pipes integration with full event reporting
    - Per-PVT incremental rerun (only failed/new PVT re-runs)
    - The fine-grain split of step4 from "all PVTs in one TCL"
      to "one PVT per partition"
    """
    keys = context.partition_key.keys_by_dimension
    corner, vt = keys["corner"], keys["volt_temp"]

    return pipes_subprocess_client.run(
        command=[
            "python3", str(SCRIPTS / "python" / "liberate_invoke.py"),
            "--corner", corner,
            "--vt", vt,
            "--netlist", str(DIR_NETLIST / f"{corner}.netlist"),
            "--out-dir", str(DIR_LIBERATE),
            "--tcl", str(SCRIPTS / "tcl" / "char_one_pvt.tcl"),
        ],
        context=context,
        env={"FORCE_RERUN": os.environ.get("FORCE_RERUN", "0")},
    ).get_materialize_result()


# Step 5 — Python: liberty_aggregate ------------------------------

@asset(partitions_def=corner_partitions, deps=[AssetKey("liberate_run")])
def liberty_aggregate(context: AssetExecutionContext) -> MaterializeResult:
    """Per-corner: collect all this corner's PVT .lib outputs and
    merge into a per-corner aggregate.
    """
    corner = context.partition_key
    libs = sorted(DIR_LIBERATE.glob(f"{corner}__*.lib"))
    expected = len(VOLT_TEMPS)  # 6
    if len(libs) < expected:
        raise RuntimeError(
            f"liberty_aggregate({corner}): expected {expected} .lib "
            f"files, got {len(libs)}",
        )
    merged_path = DIR_AGGREGATE / f"{corner}__merged.lib"
    merged_content = "\n".join(p.read_text() for p in libs)
    merged_path.write_text(
        f"// merged liberty for {corner}\n"
        f"// {len(libs)} PVTs combined\n\n"
        f"{merged_content}\n",
    )
    return MaterializeResult(
        data_version=DataVersion(_digest(merged_path)),
        metadata={
            "corner": corner,
            "pvt_count": len(libs),
            "merged_size_bytes": merged_path.stat().st_size,
        },
    )


# Step 6 — Perl: signoff_lib --------------------------------------

@asset(deps=[AssetKey("liberty_aggregate")])
def signoff_lib(context: AssetExecutionContext) -> MaterializeResult:
    """Final sign-off package: combine all per-corner aggregates."""
    output = DIR_SIGNOFF / "signoff_package.tar"
    cmd = [
        "perl", str(SCRIPTS / "perl" / "signoff.pl"),
        "--aggregate-dir", str(DIR_AGGREGATE),
        "--out", str(output),
    ]
    subprocess.run(cmd, check=True)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={
            "path": str(output),
            "size_bytes": output.stat().st_size,
        },
    )


defs = Definitions(
    assets=[
        cell_list, lpe, corner_setup, netlist_gen,
        liberate_run, liberty_aggregate, signoff_lib,
    ],
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
    },
)
