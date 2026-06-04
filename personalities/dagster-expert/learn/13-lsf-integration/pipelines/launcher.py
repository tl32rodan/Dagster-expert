"""Reference: LSFRunLauncher — the large-scale path (lesson 13 Part B).

Use this pattern when run COUNT exceeds what the orchestrator can fork
(>~thousands of concurrent runs). For ≤ hundreds of runs, keep the
DefaultRunLauncher + asset-body Pipes bsub pattern (Part A,
`pipelines/asset.py`).

This file is a corpus reference impl meant to be **read** as the
canonical RunLauncher contract. Running it end-to-end requires:
  1. real Dagster install (this is air-gap corpus material; `pip install
     dagster==1.13.3` from the wheelhouse)
  2. shared Postgres run store — remote LSF workers cannot use
     SQLite/NFS (see `learn/12-scaling/POSTGRES_MIGRATION.md`)
  3. real LSF cluster OR the mock shims in `scripts/mock_lsf/`
     (bsub + bjobs + bkill — sufficient for launcher unit-level checks)

Production-grade framework integration lives outside the corpus, in
`personalities/flow-cartographer/framework/launcher/lsf_run_launcher.py`
per the five-layer whitepaper §6.2. This file mirrors that contract for
teaching.

Standard-usage tradeoff:
- STANDARD_USAGE.md §1 table: asset-body Pipes is default ≤ hundreds of
  runs; custom launcher ONLY when run COUNT exceeds orchestrator fork
  capacity.
- §8 small/medium-scale vs large-scale regimes — this file is the
  large-scale answer.
- §9c: a custom launcher does NOT speed up backfills (run-count is
  bounded by max_concurrent_runs); the bottleneck this solves is
  orchestrator process count, not within-run parallelism.
"""

import os
import re
import subprocess

from dagster._core.launcher import (
    CheckRunHealthResult,
    LaunchRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import ConfigurableClass, ConfigurableClassData

LSF_JOB_ID_TAG = "lsf/job_id"


class LSFRunLauncher(RunLauncher, ConfigurableClass):
    """One Dagster run = one bsub = one LSF job.

    Required only when run COUNT exceeds orchestrator fork capacity.
    Requires shared Postgres run store (remote workers reach the same
    DB over internal network).
    """

    supports_check_run_worker_health = True

    def __init__(
        self,
        default_queue: str = "normal",
        default_cores: int = 4,
        default_mem_mb: int = 4096,
        default_walltime: str = "24:00",
        project: str | None = None,
        log_dir: str = "/local/dagster_home/lsf_logs",
        inst_data: ConfigurableClassData | None = None,
    ):
        self._default_queue = default_queue
        self._default_cores = default_cores
        self._default_mem_mb = default_mem_mb
        self._default_walltime = default_walltime
        self._project = project
        self._log_dir = log_dir
        self._inst_data = inst_data
        super().__init__()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        from dagster import Field, IntSource, StringSource

        return {
            "default_queue": Field(StringSource, is_required=False, default_value="normal"),
            "default_cores": Field(IntSource, is_required=False, default_value=4),
            "default_mem_mb": Field(IntSource, is_required=False, default_value=4096),
            "default_walltime": Field(StringSource, is_required=False, default_value="24:00"),
            "project": Field(StringSource, is_required=False),
            "log_dir": Field(
                StringSource,
                is_required=False,
                default_value="/local/dagster_home/lsf_logs",
            ),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run

        args = ExecuteRunArgs(
            job_origin=run.job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()

        queue = run.tags.get("lsf/queue", self._default_queue)
        cores = run.tags.get("lsf/cores", str(self._default_cores))
        mem = run.tags.get("lsf/mem_mb", str(self._default_mem_mb))
        wall = run.tags.get("lsf/walltime", self._default_walltime)
        proj = run.tags.get("lsf/project", self._project)

        os.makedirs(self._log_dir, exist_ok=True)
        out = f"{self._log_dir}/{run.run_id}.out"
        err = f"{self._log_dir}/{run.run_id}.err"

        bsub = [
            "bsub",
            "-J", f"dagster_run_{run.run_id[:8]}",
            "-q", queue,
            "-n", str(cores),
            "-R", f"rusage[mem={mem}]",
            "-W", wall,
            "-o", out,
            "-e", err,
            "-env", "DAGSTER_HOME,DAGSTER_PG_PASSWORD,PATH,PYTHONPATH",
        ]
        if proj:
            bsub += ["-P", proj]
        bsub += args

        proc = subprocess.run(bsub, capture_output=True, text=True, check=True)
        haystack = (proc.stdout or "") + "\n" + (proc.stderr or "")
        m = re.search(r"Job <(\d+)> is submitted", haystack)
        job_id = m.group(1) if m else ""

        self._instance.add_run_tags(run.run_id, {LSF_JOB_ID_TAG: job_id})
        self._instance.report_engine_event(
            f"Submitted to LSF as job {job_id} (queue={queue}, cores={cores})",
            run,
            cls=self.__class__,
        )

    def terminate(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id)
        job_id = run.tags.get(LSF_JOB_ID_TAG) if run else None
        if not job_id:
            return False
        self._instance.report_run_canceling(run)
        subprocess.run(["bkill", job_id], check=False)
        return True

    def check_run_worker_health(self, run) -> CheckRunHealthResult:
        job_id = run.tags.get(LSF_JOB_ID_TAG)
        if not job_id:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "no LSF job id on run tags")
        r = subprocess.run(
            ["bjobs", "-a", "-o", "stat exit_code", "-noheader", job_id],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "bjobs no record")
        state = r.stdout.split()[0]
        return {
            "PEND": CheckRunHealthResult(WorkerStatus.RUNNING),
            "RUN": CheckRunHealthResult(WorkerStatus.RUNNING),
            "DONE": CheckRunHealthResult(WorkerStatus.SUCCESS),
            "EXIT": CheckRunHealthResult(WorkerStatus.FAILED, "LSF job EXIT"),
            "PSUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
            "USUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
            "SSUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
        }.get(state, CheckRunHealthResult(WorkerStatus.UNKNOWN, f"LSF stat={state}"))
