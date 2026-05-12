"""Lesson 13 — LSF integration via Pipes.

Asset bodies call `lsf_submit.py` which assembles a `bsub` command
that runs the inner work on an LSF compute node. dagster_pipes
provides bidirectional event flow over shared filesystem.

For local dev: a mock `bsub` is in scripts/mock_lsf/. Prepend it
to PATH before running dagster dev or smoke:

    export PATH=$PWD/scripts/mock_lsf:$PATH
    dagster dev -m pipelines

Real LSF deployment: the mock is OFF the PATH; the real bsub is
on PATH. Same asset code, same flags.
"""

import hashlib
import os
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    DataVersion,
    Definitions,
    MaterializeResult,
    MultiPartitionsDefinition,
    PipesSubprocessClient,
    StaticPartitionsDefinition,
    asset,
)

LESSON_ROOT = Path(__file__).parent.parent
LSF_SUBMIT = LESSON_ROOT / "scripts" / "python" / "lsf_submit.py"
CHAR_INNER = LESSON_ROOT / "scripts" / "python" / "char_inner.py"
OUT_DIR = Path("/tmp/dagster-13-lsf")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CORNERS = ["ff_125", "tt_25", "ss_m40"]
VTS = ["0p9v__25c", "1p1v__125c"]

pvt_partitions = MultiPartitionsDefinition({
    "corner":    StaticPartitionsDefinition(CORNERS),
    "volt_temp": StaticPartitionsDefinition(VTS),
})


@asset(
    partitions_def=pvt_partitions,
    tags={"dagster/concurrency_key": "lsf_char"},
)
def char_via_lsf(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    """One bsub per (corner, vt) partition.

    - lsf_submit.py assembles bsub flags + forwards Pipes env
    - The inner char_inner.py runs on the LSF compute node
    - Pipes messages flow over shared FS back to Dagster
    """
    keys = context.partition_key.keys_by_dimension
    corner, vt = keys["corner"], keys["volt_temp"]

    return pipes_subprocess_client.run(
        command=[
            sys.executable, str(LSF_SUBMIT),
            "--job-name", f"char_{corner}_{vt}",
            "--queue", "normal",
            "--walltime", "00:30",
            "--memory-mb", "2048",
            "--log-dir", str(OUT_DIR / "logs"),
            "--max-pending", "100",
            "--env-mode", "pipes-only",
            "--",
            sys.executable, str(CHAR_INNER),
            "--corner", corner,
            "--vt", vt,
            "--out-dir", str(OUT_DIR / "libs"),
        ],
        context=context,
        env={"PATH": os.environ.get("PATH", "")},  # forward PATH explicitly
    ).get_materialize_result()


defs = Definitions(
    assets=[char_via_lsf],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)
