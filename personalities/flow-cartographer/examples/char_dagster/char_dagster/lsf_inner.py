"""Pipes-aware inner process — what LSF runs on the compute node.

Renders the per-(trio_group, pvt) run.scr, invokes the mock liberate via
``bsub -K`` (synchronous), then reports the asset materialization back to
Dagster over dagster-pipes. ``data_version`` is the SHA-256 digest of the
collected .ldb digests so per-partition staleness tracks CONTENT, not
the absolute paths in the .lib headers.

This file lives outside ``char_dagster/`` so it can run as a standalone
script under bsub without dragging asset / partition imports along.
"""
import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path

from dagster_pipes import open_dagster_pipes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trio-group", required=True)
    ap.add_argument("--pvt", required=True)
    ap.add_argument("--main-tcl", required=True)
    ap.add_argument("--run-scr", required=True)
    ap.add_argument("--log-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--bsub", required=True)
    ap.add_argument("--liberate", required=True)
    ap.add_argument("--queue", default="normal")
    args = ap.parse_args()

    with open_dagster_pipes() as pipes:
        log_dir = Path(args.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        env = {
            **os.environ,
            "CHAR_TRIO_GROUP": args.trio_group,
            "CHAR_PVT": args.pvt,
            "CHAR_OUT_DIR": args.out_dir,
            "CHAR_LOG_DIR": str(log_dir),
        }

        pipes.log.info(
            f"lsf_inner: submitting {args.trio_group}/{args.pvt} via bsub "
            f"(queue={args.queue}, log_dir={log_dir})"
        )
        cmd = [
            sys.executable, args.bsub,
            "-K", "-q", args.queue,
            "-J", f"char_{args.trio_group}_{args.pvt}",
            "--",
            sys.executable, args.liberate,
            "-tcl", args.main_tcl,
            "-log", str(log_dir / "liberate.log"),
        ]
        r = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if r.stdout:
            pipes.log.info(r.stdout.strip())
        if r.stderr:
            pipes.log.info(r.stderr.strip())
        if r.returncode != 0:
            pipes.log.error(f"bsub/liberate failed rc={r.returncode}")
            sys.exit(r.returncode)

        # Collect outputs for metadata + data_version.
        out_dir = Path(args.out_dir) / args.trio_group / args.pvt
        libs = sorted(out_dir.glob("*.lib"))
        ldbs = sorted(out_dir.glob("*.ldb"))

        h = hashlib.sha256()
        for ldb in ldbs:
            h.update(ldb.read_bytes())
        data_version = h.hexdigest()[:16] if ldbs else "empty"

        pipes.report_asset_materialization(
            metadata={
                "trio_group": args.trio_group,
                "pvt": args.pvt,
                "lib_count": len(libs),
                "ldb_count": len(ldbs),
                "lib_total_bytes": sum(p.stat().st_size for p in libs),
                "ldb_total_bytes": sum(p.stat().st_size for p in ldbs),
                "out_dir": str(out_dir),
                "log_dir": str(log_dir),
            },
            data_version=data_version,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
