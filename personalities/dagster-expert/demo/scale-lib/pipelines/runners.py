"""Step runners — thin Dagster resources wrapping subprocess calls to
mock perl/python scripts. Real production swap-point is documented at
the bottom of the file (the LSF bsub variant).
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, ConfigurableResource

from .folder_digest import FolderSummary, digest_folder_manifest, write_meta


_DEMO_ROOT = Path(__file__).resolve().parents[1]


class PerlRunner(ConfigurableResource):
    """Run a perl mock script for a (library, branch, step). Writes the
    step's output folder, computes its data-version digest, returns the
    summary.

    Production swap: replace ``subprocess.run([...])`` with
    ``subprocess.run(["bsub", "-K", "-J", f"{step}-{branch}", ...])`` —
    the rest of the interface stays the same.
    """

    perl_bin: str = "perl"
    output_root: str = "/tmp/dagster-scale-lib"

    def run(
        self,
        *,
        context: AssetExecutionContext,
        library: str,
        branch: str,
        step: str,
    ) -> FolderSummary:
        out_dir = Path(self.output_root) / library / branch / step
        out_dir.mkdir(parents=True, exist_ok=True)
        script = _DEMO_ROOT / "scripts" / "perl" / f"{step}.pl"
        cmd = [
            self.perl_bin, str(script),
            "--library", library,
            "--branch", branch,
            "--step", step,
            "--out", str(out_dir),
        ]
        context.log.info(f"[perl] {' '.join(cmd)}")
        subprocess.run(cmd, check=True, env={**os.environ, "PYTHONUNBUFFERED": "1"})
        summary = digest_folder_manifest(out_dir)
        write_meta(out_dir, summary)
        return summary


class PythonRunner(ConfigurableResource):
    """Run a python mock script for the chain steps step2..step5. Same
    contract as PerlRunner. Could be upgraded to Pipes (see lesson 09)
    when real scripts emit dagster_pipes events.
    """

    python_bin: str = "python3"
    output_root: str = "/tmp/dagster-scale-lib"

    def run(
        self,
        *,
        context: AssetExecutionContext,
        library: str,
        branch: str,
        step: str,
    ) -> FolderSummary:
        out_dir = Path(self.output_root) / library / branch / step
        out_dir.mkdir(parents=True, exist_ok=True)
        script = _DEMO_ROOT / "scripts" / "python" / f"{step}.py"
        cmd = [
            self.python_bin, str(script),
            "--library", library,
            "--branch", branch,
            "--step", step,
            "--out", str(out_dir),
        ]
        context.log.info(f"[python] {' '.join(cmd)}")
        subprocess.run(cmd, check=True, env={**os.environ, "PYTHONUNBUFFERED": "1"})
        summary = digest_folder_manifest(out_dir)
        write_meta(out_dir, summary)
        return summary
