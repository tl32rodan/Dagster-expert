"""Pure content generators. Vendored from converted/pipelines/generators.py
(path-free templates only; the per-leaf orchestration lives in
liberate_inner.py so the inner script stays self-contained)."""
from .render import render

TEMPLATE_TCL = (
    "# template tcl for pvt $pvt\n"
    "set_var process $process\n"
    "set_var voltage $voltage\n"
    "set_var temperature $temperature\n"
    "set_var library_name lib__$pvt\n"
)
SECTION_TCL = (
    "# section $n for pvt $pvt\n"
    "define_section $n\n"
    "set_section_param $n mode char\n"
    "set_section_param $n pvt $pvt\n"
    "set_section_param $n index ${n}0\n"
)
MODEL_TCL = (
    "# model card for pvt $pvt\n"
    "load_model ${process}_models.lib\n"
    "set_corner $pvt\n"
    "set_temp $temperature\n"
)
NETLIST_SP = (
    "* netlist for cell $cell\n"
    ".subckt $cell $pins\n"
    "* device stubs for $cell\n"
    ".ends $cell\n"
)


def gen_template(cfg, pvt: str) -> str:
    p = cfg.pvts[pvt]
    return render(TEMPLATE_TCL, pvt=pvt, process=p["process"],
                  voltage=p["voltage"], temperature=p["temperature"])


def gen_section(cfg, pvt: str, n: int) -> str:
    return render(SECTION_TCL, n=n, pvt=pvt)


def gen_model_card(cfg, pvt: str) -> str:
    p = cfg.pvts[pvt]
    return render(MODEL_TCL, pvt=pvt, process=p["process"], temperature=p["temperature"])


def gen_netlist(cfg, cell: str) -> str:
    return render(NETLIST_SP, cell=cell, pins=cfg.cells[cell]["pins"])


def gen_cell_list(cfg) -> str:
    lines = ["# Mnpvt cell list (single file; by cell)",
             "set_cell \\( " + " ".join(cfg.cell_keys) + " \\)"]
    for c in cfg.cell_keys:
        lines.append(f"set_cell_pins {c} {{ {cfg.cells[c]['pins']} }}")
    return "\n".join(lines) + "\n"


def gen_main_tcl(cfg, root: str) -> str:
    lines = ["# main.tcl -- aggregates the per-pvt template tcls (HARDCODED paths)"]
    for pvt in cfg.pvt_keys:
        lines.append(f"source {root}/templates/template_{pvt}.tcl")
    return "\n".join(lines) + "\n"
