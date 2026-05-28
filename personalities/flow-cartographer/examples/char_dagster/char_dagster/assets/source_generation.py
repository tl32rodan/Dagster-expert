"""Source-generation assets (7 logical roles).

Each asset reads one or more Jinja templates under ``templates/`` and writes
the rendered output to a deterministic path under ``cfg.paths.generated_dir``.
Substitution values come from ``CharConfig`` only — never hardcoded.

Per STANDARD_USAGE 9a:
  * pvt_section_files is FOLDER-AS-ASSET: 7 files (1 Template + 6 SECTION)
    per (trio_group, pvt) partition, returned as ONE MaterializeResult with
    a ``file_paths`` metadata entry.

Per STANDARD_USAGE 3.2 / 1.13.3 Gotcha #4:
  * Cross-dim 2D mapping (netlist_files -> pvt_section_files) uses the
    pre-computed ``NETLIST_TO_SECTION`` StaticPartitionMapping from
    ``char_dagster.spec.mappings``.

Per STANDARD_USAGE 9c:
  * ``AutomationCondition.eager()`` rides every asset so the sensor only
    needs to kick the leaf (``netlist_files``); the daemon cascades the
    rest (matches liberate-char/converted/pipelines/assets.py:97).
"""
from pathlib import Path

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from char_dagster.config import load_config
from char_dagster.partitions import (
    trio_group_partitions,
    trio_x_cell,
    trio_x_pvt,
)
from char_dagster.paths import (
    SECTION_NUMBERS,
    add_to_liberate_path,
    bolt_path,
    derive_lpe_rc,
    main_tcl_path,
    mnpvt_cell_list_path,
    model_card_path,
    netlist_path,
    section_tcl_path,
    template_dir,
    template_tcl_path,
)
from char_dagster.spec.mappings import ALL_FROM_ANY, NETLIST_TO_SECTION
from char_dagster.utils import substitute_template

CFG = load_config(
    Path(__file__).resolve().parents[2] / "config" / "char_config.yaml"
)
TEMPLATES = template_dir(CFG)
GROUP = "sources"


def _read_template(*parts) -> str:
    return TEMPLATES.joinpath(*parts).read_text()


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _meta(path: Path) -> dict:
    return {
        "path": MetadataValue.path(str(path)),
        "size_bytes": path.stat().st_size,
    }


# --- 1. add_to_liberate_tcl  (unpartitioned) --------------------------------

@asset(group_name=GROUP, automation_condition=AutomationCondition.eager())
def add_to_liberate_tcl(context: AssetExecutionContext) -> MaterializeResult:
    text = substitute_template(_read_template("add_to_liberate.tcl.j2"), {
        "project": CFG.project,
        "paths": CFG.paths,
        "tool_settings": CFG.tool_settings,
    })
    path = add_to_liberate_path(CFG)
    _write(path, text)
    return MaterializeResult(metadata=_meta(path))


# --- 2. bolt_tcl  (unpartitioned) -------------------------------------------

@asset(group_name=GROUP, automation_condition=AutomationCondition.eager())
def bolt_tcl(context: AssetExecutionContext) -> MaterializeResult:
    text = substitute_template(_read_template("Bolt.tcl.j2"), {
        "project": CFG.project,
        "paths": CFG.paths,
        "tool_settings": CFG.tool_settings,
    })
    path = bolt_path(CFG)
    _write(path, text)
    return MaterializeResult(metadata=_meta(path))


# --- 3. mnpvt_cell_list_tcl  (trio_group) -----------------------------------

@asset(
    partitions_def=trio_group_partitions,
    group_name=GROUP,
    automation_condition=AutomationCondition.eager(),
)
def mnpvt_cell_list_tcl(context: AssetExecutionContext) -> MaterializeResult:
    tg = context.partition_key
    text = substitute_template(
        _read_template(".MnPVT_cell_list", "_cell_list.tcl.j2"),
        {
            "project": CFG.project,
            "trio_group": tg,
            "lpe_rc": derive_lpe_rc(tg),
            "cells": CFG.cells,
        },
    )
    path = mnpvt_cell_list_path(CFG, tg)
    _write(path, text)
    return MaterializeResult(metadata=_meta(path))


# --- 4. model_card_files  (trio_group × pvt) --------------------------------

@asset(
    partitions_def=trio_x_pvt,
    group_name=GROUP,
    automation_condition=AutomationCondition.eager(),
)
def model_card_files(context: AssetExecutionContext) -> MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    tg, pvt_name = kd["trio_group"], kd["pvt"]
    pvt = CFG.pvt_by_name(pvt_name)
    text = substitute_template(
        _read_template("Model_card", "_card.tcl.j2"),
        {
            "project": CFG.project,
            "pvt": pvt,
            "trio_group": tg,
            "lpe_rc": derive_lpe_rc(tg),
        },
    )
    path = model_card_path(CFG, tg, pvt_name)
    _write(path, text)
    return MaterializeResult(metadata=_meta(path))


# --- 5. netlist_files  (trio_group × cell) ----------------------------------

@asset(
    partitions_def=trio_x_cell,
    group_name=GROUP,
    automation_condition=AutomationCondition.eager(),
)
def netlist_files(context: AssetExecutionContext) -> MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    tg, cell = kd["trio_group"], kd["cell"]
    text = substitute_template(
        _read_template("Netlist", "_cell.spi.j2"),
        {
            "project": CFG.project,
            "cell": cell,
            "trio_group": tg,
            "lpe_rc": derive_lpe_rc(tg),
        },
    )
    path = netlist_path(CFG, tg, cell)
    _write(path, text)
    return MaterializeResult(metadata=_meta(path))


# --- 6. pvt_section_files  (trio_group × pvt) — folder-as-asset (7 files) ---

@asset(
    partitions_def=trio_x_pvt,
    deps=[
        # 2D identity: omit partition_mapping= -> Dagster matches keys.
        AssetDep(AssetKey("model_card_files")),
        # 2D->2D cross-dim (cell -> pvt, all): pre-computed static mapping.
        AssetDep(AssetKey("netlist_files"), partition_mapping=NETLIST_TO_SECTION),
    ],
    group_name=GROUP,
    automation_condition=AutomationCondition.eager(),
)
def pvt_section_files(context: AssetExecutionContext) -> MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    tg, pvt_name = kd["trio_group"], kd["pvt"]
    pvt = CFG.pvt_by_name(pvt_name)
    written: list[Path] = []

    # Template/<library_name><pvt>.tcl
    text = substitute_template(
        _read_template("Template", "_template.tcl.j2"),
        {
            "project": CFG.project,
            "pvt": pvt,
            "trio_group": tg,
            "lpe_rc": derive_lpe_rc(tg),
        },
    )
    path = template_tcl_path(CFG, tg, pvt_name)
    _write(path, text)
    written.append(path)

    # SECTION/SECTION_{2..7}/char_<pvt>.tcl  (6 files)
    for n in SECTION_NUMBERS:
        text = substitute_template(
            _read_template(".Trio_pvt_setting", "SECTION", f"SECTION_{n}",
                           "_section.tcl.j2"),
            {
                "project": CFG.project,
                "pvt": pvt,
                "trio_group": tg,
                "lpe_rc": derive_lpe_rc(tg),
                "section_number": n,
            },
        )
        path = section_tcl_path(CFG, tg, pvt_name, n)
        _write(path, text)
        written.append(path)

    return MaterializeResult(metadata={
        "file_paths": MetadataValue.json([str(p) for p in written]),
        "file_count": len(written),
        "total_size_bytes": sum(p.stat().st_size for p in written),
    })


# --- 7. main_char_script  (unpartitioned) -----------------------------------

@asset(
    deps=[
        AssetDep(AssetKey("add_to_liberate_tcl")),
        AssetDep(AssetKey("bolt_tcl")),
        AssetDep(AssetKey("mnpvt_cell_list_tcl"), partition_mapping=ALL_FROM_ANY),
        AssetDep(AssetKey("pvt_section_files"), partition_mapping=ALL_FROM_ANY),
    ],
    group_name=GROUP,
    automation_condition=AutomationCondition.eager(),
)
def main_char_script(context: AssetExecutionContext) -> MaterializeResult:
    text = substitute_template(_read_template("main.tcl.j2"), {
        "project": CFG.project,
        "paths": CFG.paths,
        "trio_groups": CFG.trio_groups,
        "pvts": CFG.pvt_names,
        "cells": CFG.cells,
    })
    path = main_tcl_path(CFG)
    _write(path, text)
    return MaterializeResult(metadata=_meta(path))
