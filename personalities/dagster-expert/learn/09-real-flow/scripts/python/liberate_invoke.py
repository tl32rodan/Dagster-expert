"""liberate_invoke.py — Python wrapper that invokes mock TCL char tool.

Step 4 of the AP characterization flow. Per (corner, vt) — one
process per PVT, fine-grained.

Reports back to Dagster via the official `dagster_pipes` Python
client (`open_dagster_pipes()`). This is the recommended pattern
for tightly-integrated subprocess: full event log, MaterializeResult,
log forwarding, all over the Pipes IPC channel.

Real TSMC equivalent: this Python wraps a `liberate -batch -execute
char.tcl -V pvt=<key>` call. The TCL is the actual EDA tool driver.
"""

import argparse
import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

from dagster_pipes import (
    PipesContext,
    open_dagster_pipes,
)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--corner", required=True)
    p.add_argument("--vt", required=True)
    p.add_argument("--netlist", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--tcl", required=True)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    lib_path = out_dir / f"{args.corner}__{args.vt}.lib"
    checkpoint = out_dir / f".{args.corner}__{args.vt}.done"

    with open_dagster_pipes() as pipes:
        ctx: PipesContext = pipes
        ctx.log.info(
            f"liberate_invoke: corner={args.corner} vt={args.vt}"
        )

        # ── Incremental rerun check ────────────────────────────
        if checkpoint.exists() and os.environ.get("FORCE_RERUN", "0") != "1":
            ctx.log.info(
                f"checkpoint hit at {checkpoint}; skipping char "
                f"(set FORCE_RERUN=1 to redo)"
            )
            existing = lib_path.read_bytes()
            ctx.report_asset_materialization(
                data_version=_digest(existing),
                metadata={
                    "corner": args.corner,
                    "vt": args.vt,
                    "skipped": True,
                    "lib_path": str(lib_path),
                },
            )
            return 0

        # ── Read upstream netlist (Style B fan-in) ─────────────
        netlist = Path(args.netlist).read_text()
        ctx.log.info(f"loaded netlist: {len(netlist)} bytes")

        # ── Invoke mock TCL char tool ──────────────────────────
        # Real: liberate -64 -batch -execute char.tcl -V pvt=$args.vt
        ctx.log.info(f"invoking TCL: {args.tcl}")
        result = subprocess.run(
            ["tclsh", args.tcl, args.corner, args.vt, str(lib_path)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            ctx.log.error(f"TCL failed: {result.stderr}")
            return result.returncode
        ctx.log.info(f"TCL stdout: {result.stdout.strip()}")

        # ── Verify .lib was produced and write checkpoint ──────
        if not lib_path.exists():
            ctx.log.error(f"TCL did not produce {lib_path}")
            return 1

        checkpoint.write_text(time.strftime("%Y-%m-%dT%H:%M:%S\n"))
        lib_bytes = lib_path.read_bytes()

        ctx.report_asset_materialization(
            data_version=_digest(lib_bytes),
            metadata={
                "corner": args.corner,
                "vt": args.vt,
                "skipped": False,
                "lib_path": str(lib_path),
                "lib_size_bytes": len(lib_bytes),
                "checkpoint": str(checkpoint),
            },
        )
        ctx.log.info("liberate_invoke: done")
        return 0


if __name__ == "__main__":
    sys.exit(main())
