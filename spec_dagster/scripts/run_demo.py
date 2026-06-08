"""End-to-end D1 verification: daemon + sensor drives liberate-char to
completion on the spec_dagster framework (local-sim).

Steps:
  1. Set up a fresh $DAGSTER_HOME with the local-sim dagster.yaml.
  2. Bootstrap the 6 generator assets (writes SOURCES to disk + records
     materializations in the instance). characterize reads SOURCES from
     disk; it does not pull generators via IO manager (deps, not ins).
  3. Start `dagster-daemon run`. The characterize reconcile sensor
     (default RUNNING) sees 0 of 9 partitions materialized and emits 9
     RunRequests; QueuedRunCoordinator + DefaultRunLauncher execute them.
  4. Poll the instance until all 9 characterize partitions are
     materialized (or timeout), then verify artifacts + determinism.
  5. Tear the daemon down.

Run (from spec_dagster/, dagster venv):
    PYTHONPATH=$PWD python -m scripts.run_demo
"""
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent          # spec_dagster/
DAGSTER_HOME = ROOT / ".dagster_home"
VENDOR_BIN = ROOT / "flows" / "liberate_char" / "_vendor" / "bin"
LOCALSIM_YAML = ROOT / "framework" / "config" / "dagster.localsim.yaml"
WORKSPACE = ROOT / "flows" / "liberate_char" / "workspace.yaml"
DAG_ROOT = Path(os.environ.get("LIBERATE_DAG_ROOT", "/tmp/liberate-char-dag"))

PVTS = ["tt_25", "ff_125", "ss_m40"]
CELLS = ["INV", "BUF", "NAND2"]
GEN_PVT = ["template_tcl", "section_tcl", "model_card"]
TIMEOUT_S = 240
EXPECTED = 9  # 3 pvt x 3 cell


def _log(msg):
    print(f"[run_demo] {msg}", flush=True)


def setup_home():
    if DAGSTER_HOME.exists():
        shutil.rmtree(DAGSTER_HOME)
    DAGSTER_HOME.mkdir(parents=True)
    shutil.copy(LOCALSIM_YAML, DAGSTER_HOME / "dagster.yaml")
    if DAG_ROOT.exists():
        shutil.rmtree(DAG_ROOT)
    os.environ["DAGSTER_HOME"] = str(DAGSTER_HOME)
    os.environ["LIBERATE_DAG_ROOT"] = str(DAG_ROOT)


def bootstrap_generators(instance):
    from flows.liberate_char.definitions import defs

    by_name = {k.to_user_string(): a for a in defs.assets for k in a.keys}
    res = {"pipes_subprocess_client": dg.PipesSubprocessClient()}

    def mat(name, pk=None):
        r = dg.materialize([by_name[name]], partition_key=pk, resources=res,
                           selection=[name], instance=instance)
        assert r.success, f"generator FAIL {name} {pk}"

    for pvt in PVTS:
        for n in GEN_PVT:
            mat(n, pvt)
    for cell in CELLS:
        mat("netlist", cell)
    mat("cell_list")
    mat("main_tcl")
    _log("bootstrapped 11 generator materializations + SOURCES on disk")


def observed_count(instance) -> int:
    keys = instance.get_materialized_partitions(dg.AssetKey("characterize"))
    return len(keys)


def start_daemon():
    env = dict(os.environ)
    env["PATH"] = f"{VENDOR_BIN}:{env.get('PATH', '')}"   # mock bsub on PATH
    env["PYTHONPATH"] = str(ROOT)
    log_f = open(DAGSTER_HOME / "daemon.log", "w")
    daemon_bin = Path(sys.executable).parent / "dagster-daemon"  # venv's daemon CLI
    proc = subprocess.Popen(
        [str(daemon_bin), "run", "-w", str(WORKSPACE)],
        cwd=str(ROOT), env=env, stdout=log_f, stderr=subprocess.STDOUT,
    )
    _log(f"daemon started (pid {proc.pid}); log -> {DAGSTER_HOME/'daemon.log'}")
    return proc


def verify_determinism(instance):
    """Re-run the mock liberate directly on the framework-generated SOURCES
    for one leaf; the .ldb digest must match what the daemon run produced.
    Proves the generated SOURCES feed the tool identically (the converted/
    determinism contract)."""
    import flows.liberate_char.script as S

    pvt, cell = "tt_25", "INV"
    daemon_ldb = (DAG_ROOT / "out" / f"{pvt}__{cell}.ldb").read_text()
    daemon_digest = [l.split()[1] for l in daemon_ldb.splitlines() if l.startswith("digest ")][0]

    ref_root = Path("/tmp/liberate-char-ref")
    if ref_root.exists():
        shutil.rmtree(ref_root)
    work = ref_root / "work"
    out = ref_root / "out"
    work.mkdir(parents=True)
    out.mkdir(parents=True)
    # reuse the vendored inner-script generators against the SAME SOURCES
    sys.path.insert(0, str(ROOT / "flows" / "liberate_char" / "_vendor"))
    import liberate_inner as li  # type: ignore

    main_tcl = work / "main.tcl"
    run_scr = work / "run.scr"
    main_tcl.write_text(li.gen_main_tcl_leaf(str(DAG_ROOT / "SOURCES"), pvt))
    run_scr.write_text(li.gen_run_scr_leaf(str(DAG_ROOT / "SOURCES"), pvt, cell,
                                           str(out), str(main_tcl)))
    subprocess.run([sys.executable, str(S.LIBERATE_BIN), "-scr", str(run_scr)], check=True)
    ref_ldb = (out / f"{pvt}__{cell}.ldb").read_text()
    ref_digest = [l.split()[1] for l in ref_ldb.splitlines() if l.startswith("digest ")][0]

    ok = daemon_digest == ref_digest
    _log(f"determinism check {pvt}/{cell}: daemon={daemon_digest[:12]}.. "
         f"ref={ref_digest[:12]}.. -> {'MATCH' if ok else 'MISMATCH'}")
    return ok


def main() -> int:
    setup_home()
    _log(f"DAGSTER_HOME={DAGSTER_HOME}  LIBERATE_DAG_ROOT={DAG_ROOT}")

    with dg.DagsterInstance.get() as instance:
        bootstrap_generators(instance)

    daemon = start_daemon()
    try:
        deadline = time.time() + TIMEOUT_S
        last = -1
        while time.time() < deadline:
            with dg.DagsterInstance.get() as instance:
                n = observed_count(instance)
            if n != last:
                _log(f"characterize materialized: {n}/{EXPECTED}")
                last = n
            if n >= EXPECTED:
                break
            time.sleep(5)
        else:
            _log(f"TIMEOUT after {TIMEOUT_S}s at {last}/{EXPECTED}")
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=15)
        except subprocess.TimeoutExpired:
            daemon.kill()
        _log("daemon stopped")

    # ---- verify ----
    with dg.DagsterInstance.get() as instance:
        n = observed_count(instance)
        runs = instance.get_runs()
    libs = sorted((DAG_ROOT / "out").glob("*.lib"))
    ldbs = sorted((DAG_ROOT / "out").glob("*.ldb"))
    succeeded = sum(1 for r in runs if r.status == dg.DagsterRunStatus.SUCCESS)

    _log(f"RESULT: characterize partitions = {n}/{EXPECTED}")
    _log(f"        runs total={len(runs)} succeeded={succeeded}")
    _log(f"        artifacts: {len(libs)} .lib, {len(ldbs)} .ldb")
    det_ok = verify_determinism(instance) if n >= EXPECTED else False

    ok = (n == EXPECTED and len(libs) == EXPECTED and len(ldbs) == EXPECTED and det_ok)
    _log("=" * 56)
    _log(f"D1 daemon+sensor verification: {'PASS' if ok else 'FAIL'}")
    _log("=" * 56)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
