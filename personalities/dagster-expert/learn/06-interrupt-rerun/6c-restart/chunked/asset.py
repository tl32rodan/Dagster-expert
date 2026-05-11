"""6c · chunked work that drops files between sleeps.

The asset writes 5 chunk files, sleeping ~5s between each. If you
Ctrl-C dagster dev mid-run, you'll see partial chunks on disk
showing how far the worker got before dying.

Bonus: a partitioned variant (`chunked_per_pvt`) so you can launch
multiple parallel runs and orphan them all at once for try-2.
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

from dagster import (
    DataVersion,
    Definitions,
    DynamicPartitionsDefinition,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)


WORK_DIR = Path("/tmp/dagster-lab-6c")
PVTS = StaticPartitionsDefinition(["TT_25C_1V0", "FF_125C_1V1", "SS_m40C_0V9"])


@asset
def chunked_payload(context):
    """Writes 5 chunks 5s apart. Total ~30s.

    Ctrl-C dagster dev between chunks 2 and 4 to leave partial work
    on disk.
    """
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    chunks = []
    for i in range(5):
        chunk = WORK_DIR / f"chunk-{i}.txt"
        chunk.write_text(f"chunk {i} from pid {os.getpid()}\n")
        chunks.append(chunk)
        context.log.info(f"wrote {chunk}")
        time.sleep(5)

    payload = b"chunked done"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"chunks_written": len(chunks)},
    )


@asset(partitions_def=PVTS)
def chunked_per_pvt(context):
    """Same shape, but partitioned. Materialize all 3 to test
    bulk-orphan recovery.
    """
    pvt = context.partition_key
    pvt_dir = WORK_DIR / pvt
    pvt_dir.mkdir(parents=True, exist_ok=True)
    for i in range(3):
        (pvt_dir / f"chunk-{i}.txt").write_text(f"{pvt} chunk {i}\n")
        context.log.info(f"{pvt} chunk {i}")
        time.sleep(3)
    digest = hashlib.sha256(pvt.encode()).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"pvt": pvt},
    )


defs = Definitions(assets=[chunked_payload, chunked_per_pvt])
