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

# Mock LSF wrappers renamed `bsub.py` / `bjobs.py` / `bkill.py` —
# extension-less names were flagged by internal download policy. Linux
# PATH lookup doesn't auto-append `.py`, so invoke each one explicitly
# via `sys.executable`. See LESSONS L17.
BSUB = str(MOCK_LSF / "bsub.py")
BJOBS = str(MOCK_LSF / "bjobs.py")
BKILL = str(MOCK_LSF / "bkill.py")


def assert_bsub_K_runs_inline():
    """bsub -K should run the trailing command inline and return its
    exit code (this is what real bsub -K does, and what the
    Part-A asset-body Pipes bsub depends on)."""
    out_path = Path("/tmp/_smoke_launcher_bsub.out")
    out_path.unlink(missing_ok=True)
    r = subprocess.run(
        [sys.executable, BSUB, "-K", "-J", "test", "-q", "normal",
         "-o", str(out_path), "echo", "hello from mock bsub"],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"bsub -K exit={r.returncode}: {r.stderr}"
    assert "Job <" in r.stderr, f"bsub no submission line: {r.stderr!r}"
    assert out_path.exists(), f"bsub did not write -o file"
    assert "hello from mock bsub" in out_path.read_text()
    print(f"PASS bsub -K → {r.stderr.strip().splitlines()[0]}")


def assert_bjobs_done():
    """bjobs returns 'DONE 0' for any job id (sync mock model)."""
    r = subprocess.run(
        [sys.executable, BJOBS, "-a", "-o", "stat exit_code", "-noheader", "12345"],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"bjobs exit={r.returncode}"
    parts = r.stdout.split()
    assert parts[0] == "DONE", f"expected DONE, got {parts!r}"
    assert parts[1] == "0", f"expected exit 0, got {parts!r}"
    print(f"PASS bjobs 12345 → {r.stdout.strip()}")


def assert_bjobs_filters_empty():
    """bjobs -p / -r filters return empty under sync mock."""
    for flag in ("-p", "-r"):
        r = subprocess.run(
            [sys.executable, BJOBS, flag, "-u", os.environ.get("USER", "nobody")],
            capture_output=True, text=True,
        )
        assert r.returncode == 0, f"bjobs {flag} exit={r.returncode}"
        assert r.stdout.strip() == "", f"bjobs {flag} non-empty: {r.stdout!r}"
        print(f"PASS bjobs {flag} → empty")


def assert_bkill_noop():
    """bkill <id> succeeds and echoes termination message."""
    r = subprocess.run([sys.executable, BKILL, "99999"], capture_output=True, text=True)
    assert r.returncode == 0, f"bkill exit={r.returncode}"
    assert "99999" in r.stderr, f"bkill no echo: {r.stderr!r}"
    print(f"PASS bkill 99999 → {r.stderr.strip()}")


if __name__ == "__main__":
    print(f"Mock shims dir: {MOCK_LSF}")
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
