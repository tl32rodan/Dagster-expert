"""Smoke for lesson 13 Part B mock shims (bjobs / bkill / bsub-K).

Verifies that the mock LSF shims behave as the LSFRunLauncher (see
`pipelines/launcher.py`) expects when calling them from
launch_run / terminate / check_run_worker_health. Does NOT exercise
the launcher class itself — launcher integration requires a real
Dagster instance + Postgres + LSF (or a framework-level test harness,
see whitepaper §8).

For Part A end-to-end (asset-body Pipes bsub via DefaultRunLauncher),
see `_smoke.py`.
"""

import os
import subprocess
import sys
from pathlib import Path

LESSON_ROOT = Path(__file__).parent
MOCK_LSF = LESSON_ROOT / "scripts" / "mock_lsf"

os.environ["PATH"] = f"{MOCK_LSF}:{os.environ.get('PATH', '')}"
for p in [MOCK_LSF / "bsub", MOCK_LSF / "bjobs", MOCK_LSF / "bkill"]:
    os.chmod(p, 0o755)


def assert_bsub_K_runs_inline():
    """bsub -K should run the trailing command inline and return its
    exit code (this is what real bsub -K does, and what the
    Part-A asset-body Pipes bsub depends on)."""
    out_path = Path("/tmp/_smoke_launcher_bsub.out")
    out_path.unlink(missing_ok=True)
    r = subprocess.run(
        ["bsub", "-K", "-J", "test", "-q", "normal",
         "-o", str(out_path), "echo", "hello from mock bsub"],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"bsub -K exit={r.returncode}: {r.stderr}"
    assert "Job <" in r.stderr, f"bsub no submission line: {r.stderr!r}"
    assert out_path.exists(), f"bsub did not write -o file"
    assert "hello from mock bsub" in out_path.read_text()
    print(f"PASS bsub -K → {r.stderr.strip().splitlines()[0]}")


def assert_bjobs_done():
    """bjobs returns 'DONE 0' for any job id (sync mock model).

    This is what LSFRunLauncher.check_run_worker_health() reads to
    decide WorkerStatus — DONE → SUCCESS.
    """
    r = subprocess.run(
        ["bjobs", "-a", "-o", "stat exit_code", "-noheader", "12345"],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"bjobs exit={r.returncode}"
    parts = r.stdout.split()
    assert parts[0] == "DONE", f"expected DONE, got {parts!r}"
    assert parts[1] == "0", f"expected exit 0, got {parts!r}"
    print(f"PASS bjobs 12345 → {r.stdout.strip()}")


def assert_bjobs_filters_empty():
    """bjobs -p / -r filters return empty under sync mock (no in-flight)."""
    for flag in ("-p", "-r"):
        r = subprocess.run(
            ["bjobs", flag, "-u", os.environ.get("USER", "nobody")],
            capture_output=True, text=True,
        )
        assert r.returncode == 0, f"bjobs {flag} exit={r.returncode}"
        assert r.stdout.strip() == "", f"bjobs {flag} non-empty: {r.stdout!r}"
        print(f"PASS bjobs {flag} → empty")


def assert_bkill_noop():
    """bkill <id> succeeds and echoes termination message (no-op semantics)."""
    r = subprocess.run(["bkill", "99999"], capture_output=True, text=True)
    assert r.returncode == 0, f"bkill exit={r.returncode}"
    assert "99999" in r.stderr, f"bkill no echo: {r.stderr!r}"
    print(f"PASS bkill 99999 → {r.stderr.strip()}")


if __name__ == "__main__":
    print(f"Mock shims dir: {MOCK_LSF}")
    print(f"PATH front:     {os.environ['PATH'].split(':')[0]}")
    print()
    assert_bsub_K_runs_inline()
    assert_bjobs_done()
    assert_bjobs_filters_empty()
    assert_bkill_noop()
    print()
    print("=== Part B mock shims OK.")
    print("    Real launcher integration (launch_run / terminate /")
    print("    check_run_worker_health full path) is framework-level;")
    print("    see whitepaper §6.2 + §8 and pipelines/launcher.py.")
