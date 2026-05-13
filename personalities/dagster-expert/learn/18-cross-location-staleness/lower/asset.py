"""Lesson 18 — lower code location.

A partitioned `kit_summary` asset that produces a file per branch.
The file's bytes change when you bump `rev=1` to `rev=2`. The
sibling `upper` code location's downstream depends on this asset
via `deps=[AssetKey(["lib_lower", "kit_summary"])]`.
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    DataVersion,
    Definitions,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)

BRANCHES = ["corner", "lvf", "em", "ht"]
branch_partitions = StaticPartitionsDefinition(BRANCHES)

OUT_DIR = Path("/tmp/dagster-18-out/lower")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset(
    key_prefix=["lib_lower"],
    partitions_def=branch_partitions,
)
def kit_summary(context: AssetExecutionContext) -> MaterializeResult:
    key = context.partition_key
    payload = f"kit_summary__{key}__rev=1".encode()
    out = OUT_DIR / f"kit_{key}.bin"
    out.write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"path": str(out), "branch": key},
    )


defs = Definitions(assets=[kit_summary])
