"""6b · same shape as 6a but with side-effect emphasis.

This asset writes a file BEFORE its long sleep, so the lab can
demonstrate that SIGKILL leaves orphan side effects.
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

from dagster import (
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)


WORK_DIR = Path("/tmp/dagster-lab-6b")


@asset
def killable_payload(context):
    """Writes a side-effect file, then sleeps 30s.

    SIGKILL the worker mid-sleep and observe: file remains on
    disk; asset is not materialized.
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    side_effect = WORK_DIR / "side_effect.txt"
    side_effect.write_text(f"started at pid {os.getpid()}\n")
    context.log.info(f"wrote side effect at {side_effect}")

    duration = 30
    for i in range(duration):
        context.log.info(f"sleep {i + 1}/{duration}")
        time.sleep(1)

    payload = b"killable done"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"side_effect_file": str(side_effect)},
    )


defs = Definitions(assets=[killable_payload])
