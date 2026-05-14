"""Lesson 19 — AutoMaterializePolicy on partitioned assets.

Four assets on the same 4-corner partition definition:

    raw_corner ─► mid_corner_eager ─► final_corner_eager
                                  └─► final_corner_lazy

`mid_corner_eager` and `final_corner_eager` carry
`AutoMaterializePolicy.eager()`. `final_corner_lazy` carries
`.lazy()`. When you re-materialize one partition of `raw_corner`,
the daemon (running alongside dagster dev):

  - Notices `mid_corner_eager` partition X is stale.
  - Auto-launches a run materializing it.
  - That makes `final_corner_eager` partition X stale -> auto-run.
  - `final_corner_lazy` X also goes stale, but stays unmaterialized
    until something explicitly asks for it (or a downstream EAGER
    consumer asks).

Run with: dagster dev -m reactive
  (this starts BOTH the webserver and the daemon. The daemon is
   required for auto-materialize to fire.)
"""

import hashlib
import time
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    DataVersion,
    Definitions,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)

CORNERS = ["ff_125c", "tt_25c", "ss_m40c", "ss_125c"]
corner_partitions = StaticPartitionsDefinition(CORNERS)

OUT_DIR = Path("/tmp/dagster-19-out")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset(partitions_def=corner_partitions)
def raw_corner(context: AssetExecutionContext) -> MaterializeResult:
    """Root upstream. You materialize this by hand; everything
    downstream is auto-materialized."""
    key = context.partition_key
    payload = f"raw__{key}__rev=1".encode()
    (OUT_DIR / f"raw_{key}.bin").write_bytes(payload)
    return MaterializeResult(
        value=payload,
        data_version=DataVersion(_digest(payload)),
    )


@asset(
    partitions_def=corner_partitions,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def mid_corner_eager(
    context: AssetExecutionContext,
    raw_corner: bytes,
) -> MaterializeResult:
    """EAGER: daemon will re-materialize the matching partition as
    soon as upstream's partition is fresh."""
    # Sleep briefly so you can SEE the daemon tick latency in the UI.
    time.sleep(0.5)
    output = b"mid:" + raw_corner
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
        metadata={"timestamp": time.time()},
    )


@asset(
    partitions_def=corner_partitions,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def final_corner_eager(
    context: AssetExecutionContext,
    mid_corner_eager: bytes,
) -> MaterializeResult:
    """EAGER downstream. Re-runs when mid_corner_eager re-runs."""
    output = b"final_eager:" + mid_corner_eager
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
        metadata={"timestamp": time.time()},
    )


@asset(
    partitions_def=corner_partitions,
    auto_materialize_policy=AutoMaterializePolicy.lazy(),
)
def final_corner_lazy(
    context: AssetExecutionContext,
    mid_corner_eager: bytes,
) -> MaterializeResult:
    """LAZY downstream. Daemon notices it's stale but won't auto-run
    just for that — only if a downstream EAGER consumer asks, or
    something explicitly requests it."""
    output = b"final_lazy:" + mid_corner_eager
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
        metadata={"timestamp": time.time()},
    )


defs = Definitions(
    assets=[raw_corner, mid_corner_eager, final_corner_eager, final_corner_lazy],
)
