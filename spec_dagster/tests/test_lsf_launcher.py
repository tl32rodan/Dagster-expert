"""Integration tests for framework.launcher.lsf_run_launcher.LSFRunLauncher
against the mock LSF shims in tests/_mock_lsf/ (no real LSF, no real
remote Postgres). Exercises the three RunLauncher contract methods:
  - launch_run  -> bsub argv assembly + job-id persistence to run.tags
  - terminate   -> bkill of the persisted job id
  - check_run_worker_health -> bjobs state -> WorkerStatus mapping

The launcher is the framework's real implementation; only the LSF
binaries are mocked. The Dagster instance is a real local SQLite
DagsterInstance in a tmp dir.
"""
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import dagster as dg
import pytest
from dagster._core.launcher import WorkerStatus

from framework.launcher.lsf_run_launcher import LSF_JOB_ID_TAG, LSFRunLauncher

MOCK_DIR = Path(__file__).resolve().parent / "_mock_lsf"


def _noop_job():
    """Build a fresh resolved JobDefinition each call. Lesson learned:
    in 1.13.3 `define_asset_job(...)` returns an UnresolvedAssetJobDefinition
    which `instance.create_run_for_job` rejects; the @job decorator on a
    plain op gives a proper JobDefinition."""

    @dg.op
    def _noop():
        return None

    @dg.job
    def _noop_job_def():
        _noop()

    return _noop_job_def


@pytest.fixture
def env_paths(tmp_path, monkeypatch):
    """Put the mock LSF shims on PATH and point their record/state/kill
    sidecars into tmp."""
    record = tmp_path / "lsf_record"
    state = tmp_path / "lsf_state"
    killed = tmp_path / "lsf_killed.log"
    record.mkdir()
    state.mkdir()
    monkeypatch.setenv("PATH", f"{MOCK_DIR}:{os.environ['PATH']}")
    monkeypatch.setenv("MOCK_LSF_RECORD", str(record))
    monkeypatch.setenv("MOCK_LSF_STATE", str(state))
    monkeypatch.setenv("MOCK_LSF_KILLED", str(killed))
    return {"record": record, "state": state, "killed": killed, "tmp": tmp_path}


@pytest.fixture
def launcher(tmp_path):
    """A real LSFRunLauncher with bsub/bjobs/bkill resolved by PATH."""
    return LSFRunLauncher(
        default_queue="normal", default_cores=4, default_mem_mb=4096,
        default_walltime="24:00", log_dir=str(tmp_path / "lsf_logs"),
    )


@pytest.fixture
def instance(tmp_path, monkeypatch):
    """A real local SQLite DagsterInstance in tmp."""
    home = tmp_path / "dagster_home"
    home.mkdir()
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    with dg.DagsterInstance.get() as inst:
        yield inst


# ---------- pure helpers (whitepaper §6.2 inner contracts) ----------------

class _FakeRun:
    """Minimal stand-in for DagsterRun for argv assembly tests."""
    def __init__(self, run_id: str, tags=None):
        self.run_id = run_id
        self.tags = tags or {}


def test_bsub_argv_uses_tags_over_defaults(launcher):
    run = _FakeRun("r0123456abc", tags={
        "lsf/queue": "premium",
        "lsf/cores": "16",
        "lsf/mem_mb": "32768",
        "lsf/walltime": "12:00",
        "lsf/project": "EDAFLOW",
    })
    argv = launcher._bsub_argv(run, ["dagster", "api", "execute_run", "<json>"])
    assert argv[0] == "bsub"
    assert "-q" in argv and argv[argv.index("-q") + 1] == "premium"
    assert "-n" in argv and argv[argv.index("-n") + 1] == "16"
    assert any("rusage[mem=32768]" in a for a in argv)
    assert "-W" in argv and argv[argv.index("-W") + 1] == "12:00"
    assert "-P" in argv and argv[argv.index("-P") + 1] == "EDAFLOW"
    assert "-J" in argv  # bsub job name (run_id-prefixed)
    assert argv[-4:] == ["dagster", "api", "execute_run", "<json>"]


def test_bsub_argv_falls_back_to_defaults(launcher):
    run = _FakeRun("r-defaults", tags={})
    argv = launcher._bsub_argv(run, ["echo", "hi"])
    assert argv[argv.index("-q") + 1] == "normal"
    assert argv[argv.index("-n") + 1] == "4"
    assert any("rusage[mem=4096]" in a for a in argv)
    assert argv[argv.index("-W") + 1] == "24:00"
    assert "-P" not in argv  # no project default


def test_bsub_argv_env_forwarding_for_postgres(launcher):
    """Whitepaper §6.2 contract: the bsub -env list MUST forward the env
    a remote worker needs to reach the shared Postgres run+event store."""
    run = _FakeRun("r-env", tags={})
    argv = launcher._bsub_argv(run, [])
    env_val = argv[argv.index("-env") + 1]
    for required in ("DAGSTER_HOME", "DAGSTER_PG_PASSWORD", "PATH", "PYTHONPATH"):
        assert required in env_val, f"-env must forward {required}; got {env_val!r}"


@pytest.mark.parametrize("state,want_status,want_msg_fragment", [
    ("PEND",  WorkerStatus.RUNNING, None),
    ("RUN",   WorkerStatus.RUNNING, None),
    ("DONE",  WorkerStatus.SUCCESS, None),
    ("EXIT",  WorkerStatus.FAILED,  "LSF job EXIT"),
    ("PSUSP", WorkerStatus.RUNNING, None),
    ("ZAPPED", WorkerStatus.UNKNOWN, "LSF stat=ZAPPED"),
])
def test_map_state(state, want_status, want_msg_fragment):
    r = LSFRunLauncher._map_state(state)
    assert r.status == want_status
    if want_msg_fragment:
        assert want_msg_fragment in (r.msg or "")


# ---------- mock-bsub integration ----------------------------------------

def test_launch_run_persists_job_id_via_mock_bsub(launcher, instance, env_paths):
    """End-to-end (sans worker): the launcher exec'd the mock bsub, parsed
    a job id from its 'Job <NNNN> is submitted ...' line, and persisted
    it to run.tags via instance.add_run_tags."""
    # Build a run in the real instance so add_run_tags works.
    run = instance.create_run_for_job(
        job_def=_noop_job(),
        tags={"lsf/queue": "premium", "lsf/cores": "8"},
    )

    # Bypass ExecuteRunArgs (no code location wiring here) by stubbing the
    # worker command to a benign string the mock bsub will record and skip.
    ctx = MagicMock(spec=dg.core.launcher.LaunchRunContext) if False else MagicMock()
    ctx.dagster_run = run

    # Replace the ExecuteRunArgs path with a stub: monkeypatch the module-level
    # ExecuteRunArgs to return a tiny argv. Easier: directly call _bsub_argv +
    # subprocess, mirroring launch_run's flow. We patch launch_run's
    # ExecuteRunArgs at the framework module level.
    import framework.launcher.lsf_run_launcher as L

    class _StubArgs:
        def __init__(self, **kw): pass
        def get_command_args(self):
            return ["dagster", "api", "execute_run",
                    f'{{"run_id": "{run.run_id}"}}']

    L.ExecuteRunArgs = _StubArgs
    # Bind the launcher to the real instance (Dagster usually does this
    # via DagsterInstance.run_launcher; here we attach directly).
    launcher.register_instance(instance)

    launcher.launch_run(ctx)

    # Reload the run from the instance to see the new tag.
    reloaded = instance.get_run_by_id(run.run_id)
    assert LSF_JOB_ID_TAG in reloaded.tags, (
        "launcher must persist the LSF job id to run.tags so terminate / "
        "health-check survive a daemon restart"
    )
    job_id = reloaded.tags[LSF_JOB_ID_TAG]
    assert job_id and job_id.isdigit(), f"job id must be numeric; got {job_id!r}"

    # And the mock bsub recorded an argv with the queue tag the test set.
    recorded_argvs = list(env_paths["record"].glob("*.argv"))
    assert recorded_argvs, "mock bsub should have recorded an argv file"
    recorded = recorded_argvs[0].read_text()
    assert "'premium'" in recorded, "queue from run.tags should reach bsub"
    assert "'8'" in recorded, "cores from run.tags should reach bsub"


def test_terminate_bkills_persisted_job_id(launcher, instance, env_paths):
    """terminate() reads lsf/job_id from run.tags and bkills it. The mock
    bkill writes the job id it killed into MOCK_LSF_KILLED."""
    run = instance.create_run_for_job(
        job_def=_noop_job(),
        tags={LSF_JOB_ID_TAG: "424242"},
    )
    launcher.register_instance(instance)
    ok = launcher.terminate(run.run_id)
    assert ok
    assert env_paths["killed"].read_text().strip().splitlines() == ["424242"]


def test_terminate_without_job_id_returns_false(launcher, instance, env_paths):
    """A run with no LSF job id (launch_run hasn't completed) must not be
    bkilled — return False instead of raising or sending bkill to junk."""
    run = instance.create_run_for_job(
        job_def=_noop_job(),
        tags={},
    )
    launcher.register_instance(instance)
    assert launcher.terminate(run.run_id) is False
    assert not env_paths["killed"].exists() or env_paths["killed"].read_text() == ""


def test_check_run_worker_health_maps_done(launcher, instance, env_paths):
    """health-check on a DONE job -> SUCCESS."""
    run = instance.create_run_for_job(
        job_def=_noop_job(),
        tags={LSF_JOB_ID_TAG: "12345"},
    )
    # bjobs default is "DONE 0" when MOCK_LSF_STATE has no entry
    launcher.register_instance(instance)
    h = launcher.check_run_worker_health(run)
    assert h.status == WorkerStatus.SUCCESS


def test_check_run_worker_health_maps_exit(launcher, instance, env_paths):
    """health-check on an EXIT job -> FAILED (run_monitoring will mark
    the run failed)."""
    (env_paths["state"] / "99999").write_text("EXIT 1\n")
    run = instance.create_run_for_job(
        job_def=_noop_job(),
        tags={LSF_JOB_ID_TAG: "99999"},
    )
    launcher.register_instance(instance)
    h = launcher.check_run_worker_health(run)
    assert h.status == WorkerStatus.FAILED
    assert "EXIT" in (h.msg or "")


def test_check_run_worker_health_missing_tag(launcher, instance):
    """No persisted job id -> UNKNOWN (don't claim health when we don't know)."""
    run = instance.create_run_for_job(
        job_def=_noop_job(),
        tags={},
    )
    launcher.register_instance(instance)
    h = launcher.check_run_worker_health(run)
    assert h.status == WorkerStatus.UNKNOWN
