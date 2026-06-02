"""Job launchers for char_dagster — pure-Python helpers, NOT Dagster
``RunLauncher`` subclasses (forbidden per STANDARD_USAGE §8/§9c).

Two concrete launchers share one interface so asset bodies don't care
which backend is in use:

    LSFLauncher          wraps each job in ``bsub -K -q ... --`` and
                         dispatches to an LSF cluster (or the air-gap
                         ``bin/bsub`` mock).
    MultiThreadLauncher  runs each job as a subprocess in the local
                         Python process, no bsub. Useful when LSF is
                         unavailable (dev box, CI, an air-gap box
                         without the mock).

Both expose:

    submit_sync(spec)                            -> int       one job, blocking
    submit_pool(specs, *, max_workers=N)         -> dict      N jobs concurrently

Asset-body code is identical regardless of backend — swap the
constructor at module level and nothing downstream changes.

Two-layer concurrency model:

  RUN-level     QueuedRunCoordinator + ``dagster/concurrency_key``
                (in ``dagster.yaml``; bounds Dagster runs across
                partitions)
  ASSET-level   ``submit_pool`` inside one asset materialization
                (bounds jobs WITHIN one partition when work fans out)

The two layers compose: ``max_concurrent_runs × submit_pool's
max_workers`` is the theoretical upper bound on concurrent jobs.
NEVER subclass ``RunLauncher`` to try to do either of these — it is
the wrong layer.
"""
from __future__ import annotations

import os
import subprocess
import sys
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class JobSpec:
    """One job submission.

    ``command``  the inner command (what runs after any bsub wrapper)
    ``env``      merged onto ``os.environ`` so dagster-pipes vars pass
                 through
    ``log_dir``  when set, stdout / stderr are captured to
                 ``<log_dir>/<job_name>.{out,err}``
    """

    job_name: str
    command: list
    env: dict = field(default_factory=dict)
    log_dir: Optional[Path] = None


class Launcher(ABC):
    """Abstract base. Construct ONE module-level singleton per backend
    and import it from asset bodies. Do NOT instantiate inside an asset
    body — singleton identity matters for testability and for keeping
    the ThreadPool reusable across partitions."""

    @abstractmethod
    def submit_sync(self, spec: JobSpec) -> int:
        """Run one job, block until it returns. Returns the inner
        command's exit code."""

    def submit_pool(self, specs: list, *, max_workers: int = 4) -> dict:
        """Fan out N jobs concurrently, cap at ``max_workers``. Returns
        ``{job_name: exit_code}`` — dict order is completion order.

        This is the ONLY legitimate use of multi-threading at the
        char_dagster layer: fanning out a single partition's work to
        multiple jobs from inside one asset body. The ThreadPool sits
        in the asset's process — not in the Dagster daemon — so no
        ``RunLauncher`` subclass is involved.
        """
        results: dict = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self.submit_sync, s): s.job_name for s in specs}
            for fut in as_completed(futures):
                results[futures[fut]] = fut.result()
        return results


class LSFLauncher(Launcher):
    """``bsub -K -q ... -- inner_command``. Synchronous; returns the
    inner command's exit code, no bjobs polling required."""

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

    def submit_sync(self, spec: JobSpec) -> int:
        cmd = [
            sys.executable, str(self.bsub_path),
            "-K",
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
        env = {**os.environ, **(spec.env or {})}
        return subprocess.run(cmd, env=env).returncode


class MultiThreadLauncher(Launcher):
    """Run the inner command directly as a subprocess in the local
    Python process — no bsub wrapper. Thread-pool fan-out at the
    ``submit_pool`` layer gives the same parallelism semantics as the
    LSF backend, just without a cluster scheduler.

    Use this on dev boxes, CI, or any host where LSF (and even the
    ``bin/bsub`` mock) is unavailable. The asset body code is identical
    — swap ``LSFLauncher(...)`` for ``MultiThreadLauncher()`` at the
    module-level singleton and nothing downstream changes.
    """

    def submit_sync(self, spec: JobSpec) -> int:
        env = {**os.environ, **(spec.env or {})}
        stdout_fh = None
        stderr_fh = None
        try:
            if spec.log_dir is not None:
                spec.log_dir.mkdir(parents=True, exist_ok=True)
                stdout_fh = open(spec.log_dir / f"{spec.job_name}.out", "w")
                stderr_fh = open(spec.log_dir / f"{spec.job_name}.err", "w")
            return subprocess.run(
                spec.command,
                env=env,
                stdout=stdout_fh,
                stderr=stderr_fh,
            ).returncode
        finally:
            if stdout_fh is not None:
                stdout_fh.close()
            if stderr_fh is not None:
                stderr_fh.close()
