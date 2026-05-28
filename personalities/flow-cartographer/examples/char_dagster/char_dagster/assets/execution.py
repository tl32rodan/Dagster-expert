"""Execution assets (characterization_run, validation_check).

STANDARD_USAGE 8 + 9c: cluster dispatch from inside the asset body via
``PipesSubprocessClient`` -> ``bsub`` -> Pipes-aware inner process. We do
NOT subclass RunLauncher and we do NOT build a custom multi-thread
launcher (wrong layer).

One bsub per (trio_group, pvt) partition. Parallelism comes from
``QueuedRunCoordinator`` + the ``dagster/concurrency_key`` op_tag, not
from custom fan-out inside this asset.
"""
import json
import os
import sys
from pathlib import Path

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    PipesSubprocessClient,
    asset,
)

from char_dagster.config import load_config
from char_dagster.partitions import trio_x_pvt
from char_dagster.paths import (
    main_tcl_path,
    output_dir,
    run_scr_path,
    template_dir,
)
from char_dagster.utils import substitute_template

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CFG = load_config(PROJECT_ROOT / "config" / "char_config.yaml")
BIN_DIR = PROJECT_ROOT / "bin"
LSF_INNER = PROJECT_ROOT / "char_dagster" / "lsf_inner.py"
BSUB = BIN_DIR / "bsub"
LIBERATE = BIN_DIR / "liberate"
TEMPLATES = template_dir(CFG)


def _render_run_scr(trio_group: str, pvt_name: str) -> Path:
    """Write the per-partition run.scr under generated_dir; the file is the
    actual driver bsub will execute through the mock liberate."""
    pvt = CFG.pvt_by_name(pvt_name)
    log_dir = output_dir(CFG, trio_group, pvt_name) / "_log"
    text = substitute_template((TEMPLATES / "run.scr.j2").read_text(), {
        "project": CFG.project,
        "paths": CFG.paths,
        "trio_group": trio_group,
        "pvt": pvt,
        "main_tcl": str(main_tcl_path(CFG)),
        "log_dir": str(log_dir),
    })
    path = run_scr_path(CFG, trio_group, pvt_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@asset(
    partitions_def=trio_x_pvt,
    deps=[AssetDep(AssetKey("main_char_script"))],
    group_name="execution",
    automation_condition=AutomationCondition.eager(),
    op_tags={"dagster/concurrency_key": "liberate_run"},
)
def characterization_run(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    trio_group, pvt_name = kd["trio_group"], kd["pvt"]
    run_scr = _render_run_scr(trio_group, pvt_name)
    out_dir = output_dir(CFG, trio_group, pvt_name)
    log_dir = out_dir / "_log"

    return pipes_subprocess_client.run(
        command=[
            sys.executable, str(LSF_INNER),
            "--trio-group", trio_group,
            "--pvt", pvt_name,
            "--main-tcl", str(main_tcl_path(CFG)),
            "--run-scr", str(run_scr),
            "--log-dir", str(log_dir),
            "--out-dir", str(CFG.paths.output_dir),
            "--bsub", str(BSUB),
            "--liberate", str(LIBERATE),
            "--queue", "normal",
        ],
        context=context,
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(PROJECT_ROOT),
        },
    ).get_materialize_result()


@asset(
    partitions_def=trio_x_pvt,
    deps=[AssetDep(AssetKey("characterization_run"))],
    group_name="execution",
    automation_condition=AutomationCondition.eager(),
)
def validation_check(context: AssetExecutionContext) -> MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    trio_group, pvt_name = kd["trio_group"], kd["pvt"]
    out_dir = output_dir(CFG, trio_group, pvt_name)

    libs = sorted(out_dir.glob("*.lib"))
    ldbs = sorted(out_dir.glob("*.ldb"))
    lib_size = sum(p.stat().st_size for p in libs)
    ldb_size = sum(p.stat().st_size for p in ldbs)
    expected_cells = set(CFG.cells)
    seen_cells = {p.stem for p in libs}
    missing = sorted(expected_cells - seen_cells)
    ok = lib_size > 0 and ldb_size > 0 and not missing

    report = {
        "trio_group": trio_group,
        "pvt": pvt_name,
        "lib_count": len(libs),
        "ldb_count": len(ldbs),
        "lib_size": lib_size,
        "ldb_size": ldb_size,
        "missing_cells": missing,
        "ok": ok,
    }
    report_path = out_dir / "_validation.json"
    report_path.write_text(json.dumps(report, indent=2))

    if not ok:
        raise RuntimeError(f"validation failed for {trio_group}/{pvt_name}: {report}")

    return MaterializeResult(metadata={
        "report_path": MetadataValue.path(str(report_path)),
        "lib_count": len(libs),
        "ldb_count": len(ldbs),
        "lib_size": lib_size,
        "ldb_size": ldb_size,
        "ok": ok,
    })
