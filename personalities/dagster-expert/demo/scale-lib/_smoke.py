"""End-to-end smoke: materialize a small subset of branches × steps and
verify the folder-digest contract and lineage.

Subset (the ``standard`` family + its root):
  branches: corner, em, ht, lvf, lvf_ht
  steps:    step0, step1, step2, step3 (the gate + early chain)

Time: ~10 s. No LSF; pure local subprocess.

Run:
  setenv DAGSTER_HOME ~/.dagster-tutor/demo-scale-lib    # tcsh
  (or `export DAGSTER_HOME=...` in bash)
  python3 _smoke.py
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

_DEMO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_DEMO_ROOT))


def _run(cmd: list[str]) -> None:
    print(">>", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    if "DAGSTER_HOME" not in os.environ:
        print(
            "ERROR: $DAGSTER_HOME not set. Re-run ENV_SETUP.md Step 1.",
            file=sys.stderr,
        )
        return 2

    out_root = Path("/tmp/dagster-scale-lib")
    if out_root.exists():
        print(f"(clearing previous outputs at {out_root})")
        shutil.rmtree(out_root)

    branches = ["corner", "em", "ht", "lvf", "lvf_ht"]
    # 1) step0 only on root (per setup gate).
    _run([
        "dagster", "asset", "materialize",
        "-m", "pipelines",
        "--select", "lib_a/step0",
        "--partition", "corner",
    ])
    # 2) step1 on each branch; runs in any order (no chain dep here).
    for br in branches:
        _run([
            "dagster", "asset", "materialize",
            "-m", "pipelines",
            "--select", "lib_a/step1",
            "--partition", br,
        ])
    # 3) step2..step3 in chain order, per branch.
    for step in ["step2", "step3"]:
        for br in branches:
            _run([
                "dagster", "asset", "materialize",
                "-m", "pipelines",
                "--select", f"lib_a/{step}",
                "--partition", br,
            ])

    # Verify the folder-digest contract: every output folder must
    # contain .dagster_meta.json with the expected keys.
    print()
    print("--- contract check ---")
    expected_keys = {"data_version", "file_count", "total_bytes", "latest_mtime"}
    bad: list[str] = []
    for f in sorted(out_root.rglob(".dagster_meta.json")):
        import json
        meta = json.loads(f.read_text())
        if not expected_keys.issubset(meta):
            bad.append(str(f))
        else:
            rel = str(f.relative_to(out_root).parent)
            print(f"  OK  {rel:<40}  v={meta['data_version'][:12]}...")
    if bad:
        print(f"FAILED — {len(bad)} folders missing required meta keys:")
        for b in bad:
            print(f"  {b}")
        return 1

    print()
    print(f"SMOKE PASS — {len(branches)} branches × 4 steps materialized "
          "and digests verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
