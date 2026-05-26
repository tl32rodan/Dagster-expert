"""The converted asset graph.

Generators (folder-as-asset): each writes its file(s) into SOURCES, mirroring
flow-src's layout — produced from config, NOT copied, NOT read from $FLOW_SRC.

characterize (2D pvt x cell): assembles a per-leaf run.scr from the generated
sources and runs the (mock) liberate via LSF bsub through PipesSubprocessClient
(STANDARD_USAGE 8 — no custom RunLauncher). Auto-rebuild via AutomationCondition
(the current 1.13.3 API; AutoMaterializePolicy is deprecated here).
"""
import hashlib
import os
import sys

from dagster import (
    AssetExecutionContext, AutomationCondition, DataVersion, MaterializeResult,
    PipesSubprocessClient, asset,
)

from . import generators as g
from .deps import characterize_deps
from .paths import CONVERTED, LIBERATE_BIN, LSF_SUBMIT, LIBERATE_INNER, OUT, SOURCES, WORK
from .spec.partitions import CONFIG, SECTIONS, cell_partitions, pvt_partitions, pvt_x_cell

CFG = CONFIG


def _dv(text: str) -> DataVersion:
    return DataVersion(hashlib.sha256(text.encode()).hexdigest()[:16])


# ---- generator assets (folder-as-asset; write into SOURCES) ----------------

@asset(partitions_def=pvt_partitions, group_name="sources")
def template_tcl(context: AssetExecutionContext) -> MaterializeResult:
    pvt = context.partition_key
    (SOURCES / "templates").mkdir(parents=True, exist_ok=True)
    text = g.gen_template(CFG, pvt)
    (SOURCES / "templates" / f"template_{pvt}.tcl").write_text(text)
    return MaterializeResult(data_version=_dv(text), metadata={"pvt": pvt})


@asset(partitions_def=pvt_partitions, group_name="sources")
def section_tcl(context: AssetExecutionContext) -> MaterializeResult:
    pvt = context.partition_key
    d = SOURCES / "sections" / pvt
    d.mkdir(parents=True, exist_ok=True)
    blob = ""
    for n in SECTIONS:
        text = g.gen_section(CFG, pvt, n)
        (d / f"section{n}.tcl").write_text(text)
        blob += text
    return MaterializeResult(data_version=_dv(blob), metadata={"pvt": pvt, "sections": len(SECTIONS)})


@asset(partitions_def=pvt_partitions, group_name="sources")
def model_card(context: AssetExecutionContext) -> MaterializeResult:
    pvt = context.partition_key
    (SOURCES / "modelcard").mkdir(parents=True, exist_ok=True)
    text = g.gen_model_card(CFG, pvt)
    (SOURCES / "modelcard" / f"model_{pvt}.tcl").write_text(text)
    return MaterializeResult(data_version=_dv(text), metadata={"pvt": pvt})


@asset(partitions_def=cell_partitions, group_name="sources")
def netlist(context: AssetExecutionContext) -> MaterializeResult:
    cell = context.partition_key
    (SOURCES / "netlist").mkdir(parents=True, exist_ok=True)
    text = g.gen_netlist(CFG, cell)
    (SOURCES / "netlist" / f"{cell}.sp").write_text(text)
    return MaterializeResult(data_version=_dv(text), metadata={"cell": cell})


@asset(group_name="sources")
def cell_list(context: AssetExecutionContext) -> MaterializeResult:
    SOURCES.mkdir(parents=True, exist_ok=True)
    text = g.gen_cell_list(CFG)
    (SOURCES / "Mnpvt_cell_list.tcl").write_text(text)
    (SOURCES / "tool_env.csh").write_text(
        "#!/bin/csh\nsetenv LIBERATE_HOME /eda/liberate\n")
    return MaterializeResult(data_version=_dv(text), metadata={"cells": len(CFG.cell_keys)})


@asset(group_name="sources")
def main_tcl(context: AssetExecutionContext) -> MaterializeResult:
    SOURCES.mkdir(parents=True, exist_ok=True)
    text = g.gen_main_tcl(CFG, str(SOURCES))
    (SOURCES / "main.tcl").write_text(text)
    return MaterializeResult(data_version=_dv(text))


# ---- the 2D characterize asset (cross-dimension deps; bsub via Pipes) -------

@asset(
    partitions_def=pvt_x_cell,
    deps=characterize_deps(),
    automation_condition=AutomationCondition.eager(),
    group_name="characterize",
    op_tags={"dagster/concurrency_key": "liberate_run"},
)
def characterize(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    pvt, cell = kd["pvt"], kd["cell"]
    work = WORK / f"{pvt}__{cell}"
    work.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)

    return pipes_subprocess_client.run(
        command=[
            sys.executable, str(LSF_SUBMIT),
            "--job-name", f"char_{pvt}_{cell}", "--queue", "normal", "--memory-mb", "4096",
            "--",
            sys.executable, str(LIBERATE_INNER),
            "--sources-root", str(SOURCES), "--work-dir", str(work),
            "--out-dir", str(OUT), "--pvt", pvt, "--cell", cell,
            "--liberate", str(LIBERATE_BIN),
        ],
        context=context,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(CONVERTED)},
    ).get_materialize_result()
