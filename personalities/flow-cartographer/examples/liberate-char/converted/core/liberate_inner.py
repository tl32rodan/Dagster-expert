#!/usr/bin/env python3
"""Pipes-aware inner process (what LSF runs on the compute node).

It assembles the per-leaf main.tcl + run.scr from the generated SOURCES,
invokes the (mock) liberate tool, then reports the materialization back to
Dagster via dagster-pipes. data_version = the content digest from the .ldb
(path-free) so per-leaf staleness tracks content, not paths.
"""
import argparse
import subprocess
import sys
from pathlib import Path

from dagster_pipes import open_dagster_pipes

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # converted/
from pipelines.generators import gen_main_tcl_leaf, gen_run_scr_leaf  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources-root", required=True)
    ap.add_argument("--work-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--pvt", required=True)
    ap.add_argument("--cell", required=True)
    ap.add_argument("--liberate", required=True)
    args = ap.parse_args()

    with open_dagster_pipes() as pipes:
        work = Path(args.work_dir)
        work.mkdir(parents=True, exist_ok=True)
        main_tcl = work / "main.tcl"
        run_scr = work / "run.scr"
        main_tcl.write_text(gen_main_tcl_leaf(args.sources_root, args.pvt))
        run_scr.write_text(gen_run_scr_leaf(
            args.sources_root, args.pvt, args.cell, args.out_dir, str(main_tcl)))

        pipes.log.info(f"liberate_inner: characterizing {args.pvt}/{args.cell}")
        r = subprocess.run(
            [sys.executable, args.liberate, "-scr", str(run_scr)],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            pipes.log.error(f"liberate failed: {r.stderr.strip()}")
            return r.returncode
        pipes.log.info(r.stdout.strip())

        ldb = Path(args.out_dir) / f"{args.pvt}__{args.cell}.ldb"
        digest = "unknown"
        for line in ldb.read_text().splitlines():
            if line.startswith("digest "):
                digest = line.split()[1]
        pipes.report_asset_materialization(
            data_version=digest,
            metadata={"pvt": args.pvt, "cell": args.cell,
                      "lib": str(Path(args.out_dir) / f"{args.pvt}__{args.cell}.lib")},
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
