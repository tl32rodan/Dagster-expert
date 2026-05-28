"""End-to-end smoke that does NOT require Dagster to be installed.

Exercises every Jinja template by rendering each source-generation asset's
output manually, using the same substitution dicts the assets pass at run
time. Confirms zero leftover ``{{ }}`` placeholders and expected file
counts. Run via::

    python tests/_smoke.py
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from char_dagster.config import load_config  # noqa: E402
from char_dagster.paths import (  # noqa: E402
    SECTION_NUMBERS,
    add_to_liberate_path,
    bolt_path,
    derive_lpe_rc,
    main_tcl_path,
    mnpvt_cell_list_path,
    model_card_path,
    netlist_path,
    run_scr_path,
    section_tcl_path,
    template_dir,
    template_tcl_path,
)
from char_dagster.utils import substitute_template  # noqa: E402

JINJA_LEFTOVER = re.compile(r"\{\{\s*\w")


def _read(p):
    return p.read_text()


def _write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    if JINJA_LEFTOVER.search(content):
        raise AssertionError(f"{path}: leftover Jinja placeholder")


def main() -> int:
    cfg = load_config(ROOT / "config" / "char_config.yaml")

    # Override generated_dir + output_dir to a writable scratch path under ROOT
    # so the smoke is self-contained.
    scratch = ROOT / "_smoke_scratch"
    if scratch.exists():
        import shutil
        shutil.rmtree(scratch)
    scratch.mkdir()
    # The shipped config points work_dir at /work/char (the real flow's
    # location), which doesn't exist on this dev box. Override the
    # work-affecting paths to the scratch dir, and the template_dir to the
    # repo-co-located templates folder so the smoke runs anywhere.
    from dataclasses import replace
    cfg = replace(cfg, paths=replace(cfg.paths,
        generated_dir=str(scratch / "generated"),
        output_dir=str(scratch / "out"),
        work_dir=str(scratch),
        template_dir=str(ROOT / "templates"),
    ))

    T = template_dir(cfg)

    # 1. add_to_liberate.tcl
    _write(add_to_liberate_path(cfg), substitute_template(
        _read(T / "add_to_liberate.tcl.j2"),
        {"project": cfg.project, "paths": cfg.paths,
         "tool_settings": cfg.tool_settings},
    ))

    # 2. Bolt.tcl
    _write(bolt_path(cfg), substitute_template(
        _read(T / "Bolt.tcl.j2"),
        {"project": cfg.project, "paths": cfg.paths,
         "tool_settings": cfg.tool_settings},
    ))

    # 3. mnpvt_cell_list per trio_group
    for tg in cfg.trio_groups:
        _write(mnpvt_cell_list_path(cfg, tg), substitute_template(
            _read(T / ".MnPVT_cell_list" / "_cell_list.tcl.j2"),
            {"project": cfg.project, "trio_group": tg,
             "lpe_rc": derive_lpe_rc(tg), "cells": cfg.cells},
        ))

    # 4. model_card_files per (tg, pvt)
    for tg in cfg.trio_groups:
        for pvt_name in cfg.pvt_names:
            pvt = cfg.pvt_by_name(pvt_name)
            _write(model_card_path(cfg, tg, pvt_name), substitute_template(
                _read(T / "Model_card" / "_card.tcl.j2"),
                {"project": cfg.project, "pvt": pvt, "trio_group": tg,
                 "lpe_rc": derive_lpe_rc(tg)},
            ))

    # 5. netlist_files per (tg, cell)
    for tg in cfg.trio_groups:
        for cell in cfg.cells:
            _write(netlist_path(cfg, tg, cell), substitute_template(
                _read(T / "Netlist" / "_cell.spi.j2"),
                {"project": cfg.project, "cell": cell, "trio_group": tg,
                 "lpe_rc": derive_lpe_rc(tg)},
            ))

    # 6. pvt_section_files per (tg, pvt): 1 Template + 6 SECTION
    for tg in cfg.trio_groups:
        for pvt_name in cfg.pvt_names:
            pvt = cfg.pvt_by_name(pvt_name)
            ctx = {"project": cfg.project, "pvt": pvt, "trio_group": tg,
                   "lpe_rc": derive_lpe_rc(tg)}
            _write(template_tcl_path(cfg, tg, pvt_name), substitute_template(
                _read(T / "Template" / "_template.tcl.j2"), ctx,
            ))
            for n in SECTION_NUMBERS:
                _write(section_tcl_path(cfg, tg, pvt_name, n), substitute_template(
                    _read(T / ".Trio_pvt_setting" / "SECTION"
                          / f"SECTION_{n}" / "_section.tcl.j2"),
                    {**ctx, "section_number": n},
                ))

    # 7. main.tcl
    _write(main_tcl_path(cfg), substitute_template(
        _read(T / "main.tcl.j2"),
        {"project": cfg.project, "paths": cfg.paths,
         "trio_groups": cfg.trio_groups, "pvts": cfg.pvt_names,
         "cells": cfg.cells},
    ))

    # 8. characterization_run: write run.scr per partition + invoke mock liberate
    import os
    import subprocess
    bsub = ROOT / "bin" / "bsub"
    liberate = ROOT / "bin" / "liberate"

    for tg in cfg.trio_groups:
        for pvt_name in cfg.pvt_names:
            pvt = cfg.pvt_by_name(pvt_name)
            log_dir = Path(cfg.paths.output_dir) / tg / pvt_name / "_log"
            scr = run_scr_path(cfg, tg, pvt_name)
            _write(scr, substitute_template(
                _read(T / "run.scr.j2"),
                {"project": cfg.project, "paths": cfg.paths,
                 "trio_group": tg, "pvt": pvt,
                 "main_tcl": str(main_tcl_path(cfg)),
                 "log_dir": str(log_dir)},
            ))
            env = {
                **os.environ,
                "CHAR_TRIO_GROUP": tg, "CHAR_PVT": pvt_name,
                "CHAR_OUT_DIR": cfg.paths.output_dir,
                "CHAR_LOG_DIR": str(log_dir),
            }
            r = subprocess.run(
                [sys.executable, str(bsub), "-K", "--",
                 sys.executable, str(liberate), "-tcl", str(main_tcl_path(cfg)),
                 "-log", str(log_dir / "liberate.log")],
                env=env, capture_output=True, text=True,
            )
            if r.returncode != 0:
                print(r.stdout); print(r.stderr, file=sys.stderr)
                raise SystemExit(f"liberate failed for {tg}/{pvt_name}")

    # 9. verify outputs: per (tg, pvt), expect one .lib + .ldb per cell
    expected_files = len(cfg.trio_groups) * len(cfg.pvt_names) * len(cfg.cells)
    libs = list(Path(cfg.paths.output_dir).rglob("*.lib"))
    ldbs = list(Path(cfg.paths.output_dir).rglob("*.ldb"))
    assert len(libs) == expected_files, (
        f"expected {expected_files} .lib, got {len(libs)}"
    )
    assert len(ldbs) == expected_files, (
        f"expected {expected_files} .ldb, got {len(ldbs)}"
    )
    print(f"smoke OK: {len(libs)} .lib + {len(ldbs)} .ldb under {cfg.paths.output_dir}")
    print(f"        ({len(cfg.trio_groups)} trio_groups × "
          f"{len(cfg.pvt_names)} pvts × {len(cfg.cells)} cells)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
