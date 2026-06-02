"""LSF submission helper — a PURE PYTHON helper, NOT a Dagster RunLauncher.

Hard rule (STANDARD_USAGE §8/§9c, plan §13, mock-char-dagster skill):
the right place for cluster dispatch is INSIDE an asset body via
``PipesSubprocessClient`` → ``bsub``. Custom ``RunLauncher`` subclasses
are forbidden — they run on the wrong layer (per-Dagster-run, before
asset / partition / Pipes machinery), break subprocess isolation, and
duplicate what ``QueuedRunCoordinator`` already does.

Two-layer concurrency model this class plugs into:

  1. RUN-LEVEL parallelism — bounded by ``QueuedRunCoordinator`` +
     ``dagster/concurrency_key`` op_tag in ``dagster.yaml``. One bsub
     per partition is the default char_dagster shape.

  2. ASSET-BODY parallelism — when a single partition's work itself
     fans out (e.g. per-cell within one (trio_group, pvt) partition),
     use ``submit_pool`` to issue N concurrent bsubs from inside the
     asset materialization, capped by ``max_workers``.

Both layers compose: QueuedRunCoordinator caps how many partition runs
go simultaneously; ``submit_pool`` caps how many bsubs each partition
fans out internally.
"""
from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class LSFJobSpec:
    """One bsub submission. ``command`` is the inner command list (what
    runs after ``bsub -- ``); ``env`` is merged onto ``os.environ`` so
    dagster-pipes vars pass through."""

    job_name: str
    command: list
    env: dict = field(default_factory=dict)
    log_dir: Path | None = None


class LSFLauncher:
    """Module-level singleton (one instance per (queue, memory profile)).

    Construct once at import time in ``char_dagster/spec/launchers.py``
    or alongside the asset that uses it; do NOT instantiate inside an
    asset body (singleton identity matters for testability).
    """

    def __init__(
        self,
        bsub_path: Path,
        *,
        queue: str = "normal",
        memory_mb: int = 4096,
        walltime: str = "00:30",
    ):
        self.bsub_path = Path(bsub_path)
        self.queue = queue
        self.memory_mb = memory_mb
        self.walltime = walltime

    # ------- core submission --------------------------------------------

    def _build_cmd(self, spec: LSFJobSpec) -> list:
        cmd = [
            sys.executable, str(self.bsub_path),
            "-K",                       # synchronous (mock); real bsub -K too
            "-q", self.queue,
            "-J", spec.job_name,
            "-M", str(self.memory_mb),
            "-W", self.walltime,
        ]
        if spec.log_dir is not None:
            spec.log_dir.mkdir(parents=True, exist_ok=True)
            cmd += [
                "-o", str(spec.log_dir / f"{spec.job_name}.out"),
                "-e", str(spec.log_dir / f"{spec.job_name}.err"),
            ]
        cmd += ["--"] + list(spec.command)
        return cmd

    def submit_sync(self, spec: LSFJobSpec) -> int:
        """One synchronous bsub. Returns the inner command's exit code.

        Use this when the asset body wraps it via PipesSubprocessClient
        (matches the existing single-bsub shape in
        char_dagster/lsf_inner.py)."""
        env = {**os.environ, **(spec.env or {})}
        return subprocess.run(self._build_cmd(spec), env=env).returncode

    # ------- fan-out within one asset materialization -------------------

    def submit_pool(
        self,
        specs: list,
        *,
        max_workers: int = 4,
    ) -> dict:
        """Submit many specs concurrently, cap at ``max_workers``. Returns
        {job_name: exit_code}.

        This is the ONLY legitimate use of multi-threading in the
        char_dagster shape: fanning out a single partition's work to
        multiple bsubs from inside one asset body. The ThreadPool sits
        in the asset's process, not in the Dagster daemon — there is no
        RunLauncher subclass involved.
        """
        results: dict = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self.submit_sync, s): s.job_name for s in specs}
            for fut in as_completed(futures):
                results[futures[fut]] = fut.result()
        return results
