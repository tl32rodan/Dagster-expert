"""char_inner.py — the actual work that runs ON the LSF compute node.

Opens dagster_pipes (so this asset's events flow back to the
Dagster host via shared NFS), does mock characterization work,
reports MaterializeResult.
"""

import argparse
import hashlib
import sys
import time
from pathlib import Path

from dagster_pipes import open_dagster_pipes


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--corner", required=True)
    p.add_argument("--vt", required=True)
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()

    with open_dagster_pipes() as pipes:
        pipes.log.info(f"char_inner running on (mock) LSF host: "
                        f"corner={args.corner} vt={args.vt}")

        time.sleep(0.5)  # simulate characterization work

        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        lib = out_dir / f"{args.corner}__{args.vt}.lib"
        payload = (f"library ({args.corner}.{args.vt}) {{\n"
                   f"  /* mock liberty produced by LSF-dispatched char_inner */\n"
                   f"}}\n").encode()
        lib.write_bytes(payload)

        pipes.report_asset_materialization(
            data_version=hashlib.sha256(payload).hexdigest()[:16],
            metadata={
                "corner": args.corner,
                "vt": args.vt,
                "lib_path": str(lib),
                "lib_size_bytes": len(payload),
                "exec_host": "lsf-mock-node",
            },
        )
        pipes.log.info("char_inner done")
        return 0


if __name__ == "__main__":
    sys.exit(main())
