"""End-to-end Execution Fabric demo (WHITEPAPER §4.5, §9).

Steps:
  1. Set up a fresh $DAGSTER_HOME with `dagster.fabric.yaml`.
  2. Put the mock `bsub` on PATH (fork inner cmd, return immediately).
  3. Bootstrap generators via dg.materialize (in-process, cheap).
  4. Start dagster-daemon. The dispatch sensor emits 9 RunRequests; each
     compute asset body fires a non-blocking bsub via lsf_run_client.
     Mock bsub spawns fabric_worker.py per partition; each writes SUCCESS
     + real data_version to status DB. The harvest sensor then emits 9
     AssetMaterializations to Dagster's event log.
  5. Poll until: 9 SUCCESS in status DB AND 9 materializations in Dagster.
  6. Tear daemon down.

Run (from execution_fabric/, dagster venv):
    PYTHONPATH=$PWD python -m scripts.run_demo
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent          # execution_fabric/
DAGSTER_HOME = ROOT / ".dagster_home"
VENDOR_BIN = ROOT / "flows" / "liberate_char" / "_vendor" / "bin"
FABRIC_YAML = ROOT / "framework" / "config" / "dagster.fabric.yaml"
WORKSPACE = ROOT / "flows" / "liberate_char" / "workspace.yaml"
DAG_ROOT = Path(os.environ.get("LIBERATE_DAG_ROOT", "/tmp/liberate-char-dag"))
STATUS_DB = DAGSTER_HOME / "fabric_status.db"

PVTS = ["tt_25", "ff_125", "ss_m40"]
CELLS = ["INV", "BUF", "NAND2"]
EXPECTED = 9
TIMEOUT_S = 300


def _log(msg): print(f"[run_demo] {msg}", flush=True)


def setup_home():
    if DAGSTER_HOME.exists():
        shutil.rmtree(DAGSTER_HOME)
    DAGSTER_HOME.mkdir(parents=True)
    shutil.copy(FABRIC_YAML, DAGSTER_HOME / "dagster.yaml")
    if DAG_ROOT.exists():
        shutil.rmtree(DAG_ROOT)
    os.environ["DAGSTER_HOME"] = str(DAGSTER_HOME)
    os.environ["LIBERATE_DAG_ROOT"] = str(DAG_ROOT)
    os.environ["FABRIC_STATUS_DB"] = str(STATUS_DB)


def materialize_generators(instance):
    """Bootstrap: write SOURCES files directly via script.py's pure
    functions. We don't go through dg.materialize because the 6
    generators have mixed partition shapes (some [pvt], some [cell],
    some []), and dg.materialize's ephemeral job won't accept mixed
    shapes in one selection. The generators' data_versions don't gate
    anything downstream — the dispatch sensor reads SUCCESS from the
    status DB; harvest reports characterize materializations directly.
    """
    from flows.liberate_char import script as S
    DAG_ROOT.mkdir(parents=True, exist_ok=True)

    def _write(files: dict[str, str]):
        for path, content in files.items():
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)

    for pvt in PVTS:
        _write(S.gen_template(pvt))
        _write(S.gen_sections(pvt))
        _write(S.gen_modelcard(pvt))
    for cell in CELLS:
        _write(S.gen_netlist(cell))
    _write(S.gen_cell_list())
    _write(S.gen_main_tcl())
    _log("bootstrapped: 6 generators × N partitions wrote SOURCES files")


def count_success_in_status_db() -> int:
    if not STATUS_DB.exists():
        return 0
    with sqlite3.connect(STATUS_DB) as conn:
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE asset_name=? AND state=?",
            ("characterize", "SUCCESS"),
        ).fetchone()
    return n


def count_dagster_materializations(instance) -> int:
    return len(instance.get_materialized_partitions(dg.AssetKey("characterize")))


def start_daemon():
    env = dict(os.environ)
    env["PATH"] = f"{VENDOR_BIN}:{env.get('PATH', '')}"   # mock bsub on PATH
    env["PYTHONPATH"] = str(ROOT)
    log_f = open(DAGSTER_HOME / "daemon.log", "w")
    daemon_bin = Path(sys.executable).parent / "dagster-daemon"
    proc = subprocess.Popen(
        [str(daemon_bin), "run", "-w", str(WORKSPACE)],
        cwd=str(ROOT), env=env, stdout=log_f, stderr=subprocess.STDOUT,
    )
    _log(f"daemon started (pid {proc.pid}); log -> {DAGSTER_HOME/'daemon.log'}")
    return proc


def main() -> int:
    setup_home()
    _log(f"DAGSTER_HOME={DAGSTER_HOME}")
    _log(f"LIBERATE_DAG_ROOT={DAG_ROOT}")
    _log(f"FABRIC_STATUS_DB={STATUS_DB}")

    with dg.DagsterInstance.get() as instance:
        materialize_generators(instance)

    daemon = start_daemon()
    try:
        deadline = time.time() + TIMEOUT_S
        last = (-1, -1)
        while time.time() < deadline:
            sdb = count_success_in_status_db()
            with dg.DagsterInstance.get() as instance:
                dmat = count_dagster_materializations(instance)
            if (sdb, dmat) != last:
                _log(f"status_db SUCCESS={sdb}/{EXPECTED}  "
                     f"dagster mats={dmat}/{EXPECTED}")
                last = (sdb, dmat)
            if sdb >= EXPECTED and dmat >= EXPECTED:
                break
            time.sleep(5)
        else:
            _log(f"TIMEOUT at status_db={last[0]}/{EXPECTED} "
                 f"dagster={last[1]}/{EXPECTED}")
    finally:
        daemon.terminate()
        try: daemon.wait(timeout=15)
        except subprocess.TimeoutExpired: daemon.kill()
        _log("daemon stopped")

    # ---- verify ----
    sdb = count_success_in_status_db()
    with dg.DagsterInstance.get() as instance:
        dmat = count_dagster_materializations(instance)
    ldbs = sorted((DAG_ROOT / "out").glob("*.ldb"))
    libs = sorted((DAG_ROOT / "out").glob("*.lib"))

    _log(f"RESULT: status_db SUCCESS = {sdb}/{EXPECTED}")
    _log(f"        dagster materializations = {dmat}/{EXPECTED}")
    _log(f"        artifacts: {len(libs)} .lib, {len(ldbs)} .ldb")
    ok = sdb == EXPECTED and dmat == EXPECTED and len(ldbs) == EXPECTED
    _log("=" * 56)
    _log(f"Execution Fabric demo: {'PASS' if ok else 'FAIL'}")
    _log("=" * 56)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
