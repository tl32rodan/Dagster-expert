#!/usr/bin/env python3
"""End-to-end smoke + the correctness gate.

1. Materialize every generator partition (writes SOURCES from config).
2. Materialize every characterize leaf (pvt x cell) -> .lib/.ldb via bsub+Pipes.
3. diff-proof: assert the Dagster outputs equal the reference flow-src outputs,
   ignoring only the path-bearing header lines.

Run with the dagster venv from converted/:
    LIBERATE_DAG_ROOT=/tmp/liberate-char-dag python -m _smoke
The reference outputs must exist first: flow-src/run_all.sh
"""
import os
import sys
from pathlib import Path

CONVERTED = Path(__file__).resolve().parent
sys.path.insert(0, str(CONVERTED))
os.environ.setdefault("LIBERATE_DAG_ROOT", "/tmp/liberate-char-dag")
os.environ.setdefault("PYTHONPATH", str(CONVERTED))

from dagster import materialize, PipesSubprocessClient  # noqa: E402

from core import diff_proof                              # noqa: E402
from pipelines.assets import (                            # noqa: E402
    cell_list, characterize, main_tcl, model_card, netlist, section_tcl, template_tcl,
)
from pipelines.spec.partitions import CELLS, PVTS, pvt_x_cell  # noqa: E402

RESOURCES = {"pipes_subprocess_client": PipesSubprocessClient()}


def _mat(assets, **kw):
    result = materialize(assets, resources=RESOURCES, **kw)
    if not result.success:
        raise SystemExit(f"materialize failed: {[a.key.to_user_string() for a in assets]} {kw}")


def main() -> int:
    # 1) generators
    for pvt in PVTS:
        _mat([template_tcl, section_tcl, model_card], partition_key=pvt)
    for cell in CELLS:
        _mat([netlist], partition_key=cell)
    _mat([cell_list, main_tcl])

    # 2) characterize every (pvt, cell) leaf
    for key in pvt_x_cell.get_partition_keys():
        _mat([characterize], partition_key=key)

    # 3) the gate: products differ only in paths
    ref = "/tmp/liberate-char-ref/out"
    dag = os.path.join(os.environ["LIBERATE_DAG_ROOT"], "out")
    ok, diffs = diff_proof.outputs_equivalent(ref, dag)
    print("\n=== DIFF-PROOF ===")
    if ok:
        print("PASS: Dagster outputs == reference outputs (only embedded paths differ)")
    else:
        print("FAIL:")
        for d in diffs:
            print("  -", d)
        return 1
    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
