"""liberate-char application script (the flow owner's deliverable).

Pure functions, NO dagster import. The framework (M2 builder) calls these:
  - generator functions return {abs_path: content}; the framework writes
    the files and computes the content_hash data_version.
  - the compute function returns an argv list; the framework runs it via
    PipesSubprocessClient. NO bsub in here at framework scale — but in
    local-sim we keep the mock `bsub` on PATH as the dispatch shim, mirroring
    the converted/ reference. (At real LSF scale the bsub moves to the M4
    LSFRunLauncher and this command drops the bsub prefix — whitepaper §6.2.)

Paths come from $LIBERATE_DAG_ROOT (default /tmp/liberate-char-dag), shared
by the generator writes and the characterize reads.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from flows.liberate_char._vendor.config import load_config
from flows.liberate_char._vendor import generators as g

_HERE = Path(__file__).resolve().parent
_VENDOR = _HERE / "_vendor"
CFG = load_config(_VENDOR / "liberate.yaml")

BSUB = _VENDOR / "bin" / "bsub"
LIBERATE_BIN = _VENDOR / "bin" / "liberate"
LIBERATE_INNER = _VENDOR / "liberate_inner.py"


def _root() -> Path:
    return Path(os.environ.get("LIBERATE_DAG_ROOT", "/tmp/liberate-char-dag"))


def _sources() -> Path:
    return _root() / "SOURCES"


def _out() -> Path:
    return _root() / "out"


def _work() -> Path:
    return _root() / "work"


# ---- generator functions (return {abs_path: content}) ----------------------

def gen_template(pvt: str) -> dict[str, str]:
    return {str(_sources() / "templates" / f"template_{pvt}.tcl"): g.gen_template(CFG, pvt)}


def gen_sections(pvt: str) -> dict[str, str]:
    d = _sources() / "sections" / pvt
    return {str(d / f"section{n}.tcl"): g.gen_section(CFG, pvt, n) for n in CFG.sections}


def gen_modelcard(pvt: str) -> dict[str, str]:
    return {str(_sources() / "modelcard" / f"model_{pvt}.tcl"): g.gen_model_card(CFG, pvt)}


def gen_netlist(cell: str) -> dict[str, str]:
    return {str(_sources() / "netlist" / f"{cell}.sp"): g.gen_netlist(CFG, cell)}


def gen_cell_list() -> dict[str, str]:
    return {
        str(_sources() / "Mnpvt_cell_list.tcl"): g.gen_cell_list(CFG),
        str(_sources() / "tool_env.csh"): "#!/bin/csh\nsetenv LIBERATE_HOME /eda/liberate\n",
    }


def gen_main_tcl() -> dict[str, str]:
    return {str(_sources() / "main.tcl"): g.gen_main_tcl(CFG, str(_sources()))}


# ---- compute function (returns an argv list) -------------------------------

def characterize_command(pvt: str, cell: str) -> list[str]:
    work = _work() / f"{pvt}__{cell}"
    return [
        str(BSUB), "-K",
        "--job-name", f"char_{pvt}_{cell}", "--queue", "normal", "--memory-mb", "4096",
        "--",
        sys.executable, str(LIBERATE_INNER),
        "--sources-root", str(_sources()),
        "--work-dir", str(work),
        "--out-dir", str(_out()),
        "--pvt", pvt, "--cell", cell,
        "--liberate", str(LIBERATE_BIN),
    ]
