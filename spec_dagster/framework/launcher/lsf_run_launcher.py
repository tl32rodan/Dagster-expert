"""M4 — Custom LSFRunLauncher (whitepaper §6.2).

One Dagster run = one bsub = one LSF job. Use this when run COUNT exceeds
what the orchestrator host can fork (>~thousands of concurrent runs);
otherwise stay on DefaultRunLauncher + asset-body Pipes bsub.

Contract:
  - launch_run(LaunchRunContext) bsubs `dagster api execute_run <argv>`,
    parses the LSF job id from bsub's "Job <NNNN> is submitted" line,
    persists it to run.tags["lsf/job_id"] so terminate / health-check can
    find it after a daemon restart.
  - terminate(run_id) reads the job id from run.tags and bkills it.
  - check_run_worker_health(run) bjobs the job id and maps LSF state to
    WorkerStatus for the run_monitoring daemon
    (PEND/RUN/PSUSP/USUSP/SSUSP -> RUNNING, DONE -> SUCCESS, EXIT -> FAILED).
  - Required: shared Postgres run+event store (remote LSF workers cannot
    use SQLite-on-NFS). See learn/12-scaling/POSTGRES_MIGRATION.md.

Per-asset resources (queue/cores/mem_mb/walltime) flow from spec.lsf
through op_tags into run.tags; the launcher reads them.

This module is unit-tested AND integration-tested against the mock LSF
shims at flows/liberate_char/_vendor/bin/ (bsub + bjobs + bkill) — see
tests/test_lsf_launcher.py.
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
    supports_check_run_worker_health = True

    def __init__(
        self,
        default_queue: str = "normal",
        default_cores: int = 4,
        default_mem_mb: int = 4096,
        default_walltime: str = "24:00",
        project: str | None = None,
        log_dir: str = "/local/dagster_home/lsf_logs",
        bsub_bin: str = "bsub",
        bjobs_bin: str = "bjobs",
        bkill_bin: str = "bkill",
        tool_invoker: list[str] | None = None,
        # ^ prepended to every bsub/bjobs/bkill call. Default empty (real
        # LSF binaries on PATH). When the "binaries" are actually Python
        # wrappers — e.g. the mock {bsub,bjobs,bkill}.py shipped for tests
        # because internal download policy bans extension-less executables —
        # set to e.g. ["python3"] so subprocess.run([python3, /path/bsub.py,
        # ...]) works. See LESSONS.md L17.
        inst_data: ConfigurableClassData | None = None,
    ):
        self._default_queue = default_queue
        self._default_cores = default_cores
        self._default_mem_mb = default_mem_mb
        self._default_walltime = default_walltime
        self._project = project
        self._log_dir = log_dir
        self._bsub = bsub_bin
        self._bjobs = bjobs_bin
        self._bkill = bkill_bin
        self._invoker = list(tool_invoker or [])
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
                StringSource, is_required=False,
                default_value="/local/dagster_home/lsf_logs",
            ),
            "bsub_bin": Field(StringSource, is_required=False, default_value="bsub"),
            "bjobs_bin": Field(StringSource, is_required=False, default_value="bjobs"),
            "bkill_bin": Field(StringSource, is_required=False, default_value="bkill"),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    # ----- bsub argv assembly: pure-ish, easy to unit-test -----
    def _bsub_argv(self, run, worker_argv: list[str]) -> list[str]:
        queue = run.tags.get("lsf/queue", self._default_queue)
        cores = run.tags.get("lsf/cores", str(self._default_cores))
        mem = run.tags.get("lsf/mem_mb", str(self._default_mem_mb))
        wall = run.tags.get("lsf/walltime", self._default_walltime)
        proj = run.tags.get("lsf/project", self._project)
        os.makedirs(self._log_dir, exist_ok=True)
        out = os.path.join(self._log_dir, f"{run.run_id}.out")
        err = os.path.join(self._log_dir, f"{run.run_id}.err")
        argv = [
            *self._invoker,
            self._bsub,
            "-J", f"dagster_run_{run.run_id[:8]}",
            "-q", queue, "-n", str(cores),
            "-R", f"rusage[mem={mem}]", "-W", wall,
            "-o", out, "-e", err,
            "-env", "DAGSTER_HOME,DAGSTER_PG_PASSWORD,PATH,PYTHONPATH",
        ]
        if proj:
            argv += ["-P", proj]
        argv += worker_argv
        return argv

    # ----- LSF state -> WorkerStatus: pure, easy to test -----
    @staticmethod
    def _map_state(state: str) -> CheckRunHealthResult:
        return {
            "PEND": CheckRunHealthResult(WorkerStatus.RUNNING),
            "RUN": CheckRunHealthResult(WorkerStatus.RUNNING),
            "DONE": CheckRunHealthResult(WorkerStatus.SUCCESS),
            "EXIT": CheckRunHealthResult(WorkerStatus.FAILED, "LSF job EXIT"),
            "PSUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
            "USUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
            "SSUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
        }.get(state, CheckRunHealthResult(WorkerStatus.UNKNOWN, f"LSF stat={state}"))

    # ----- RunLauncher contract -----
    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        args = ExecuteRunArgs(
            job_origin=run.job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()

        argv = self._bsub_argv(run, args)
        proc = subprocess.run(argv, capture_output=True, text=True, check=True)
        # Real bsub prints "Job <NNNN> is submitted ..." to stderr; tolerate either.
        haystack = (proc.stdout or "") + "\n" + (proc.stderr or "")
        m = re.search(r"Job <(\d+)> is submitted", haystack)
        job_id = m.group(1) if m else ""

        self._instance.add_run_tags(run.run_id, {LSF_JOB_ID_TAG: job_id})
        self._instance.report_engine_event(
            f"Submitted to LSF as job {job_id}",
            run, cls=self.__class__,
        )

    def terminate(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id)
        job_id = run.tags.get(LSF_JOB_ID_TAG) if run else None
        if not job_id:
            return False
        self._instance.report_run_canceling(run)
        subprocess.run([*self._invoker, self._bkill, job_id], check=False)
        return True

    def check_run_worker_health(self, run) -> CheckRunHealthResult:
        job_id = run.tags.get(LSF_JOB_ID_TAG)
        if not job_id:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "no LSF job id on run tags")
        r = subprocess.run(
            [*self._invoker, self._bjobs, "-a", "-o", "stat exit_code", "-noheader", job_id],
            capture_output=True, text=True,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "bjobs no record")
        return self._map_state(r.stdout.split()[0])
