"""17a · identity partition mapping — the default cross-partition wiring.

Two assets, same partition definition. Default `IdentityPartitionMapping`
maps `upstream_ff_125c -> downstream_ff_125c`, etc.

Goal of this sub-lab: when upstream's *one* partition is re-materialized
with new content, ONLY the corresponding downstream partition goes stale
in the UI — not all 4.

Run with: dagster dev -m identity
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)

CORNERS = ["ff_125c", "tt_25c", "ss_m40c", "ss_125c"]
corner_partitions = StaticPartitionsDefinition(CORNERS)

OUT_DIR = Path("/tmp/dagster-17a-out")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset(partitions_def=corner_partitions)
def raw_corner(context: AssetExecutionContext) -> MaterializeResult:
    """Upstream. Output is a small file whose bytes change when the user
    edits this script's `payload` line. data_version is the hash of those
    bytes, so re-materializing with the same code yields the same version
    (no spurious stale)."""
    key = context.partition_key
    payload = f"raw_corner__{key}__rev=1".encode()
    out = OUT_DIR / f"raw_{key}.bin"
    out.write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"path": str(out), "size": len(payload)},
    )


@asset(
    partitions_def=corner_partitions,
    deps=[AssetKey("raw_corner")],
)
def mid_corner(context: AssetExecutionContext) -> MaterializeResult:
    """Downstream. Same partition def → default IdentityPartitionMapping.
    Reads ITS OWN partition's upstream output file (Style B) and hashes
    those bytes into its data_version. That's what makes staleness
    propagate per-partition."""
    key = context.partition_key
    upstream = (OUT_DIR / f"raw_{key}.bin").read_bytes()
    out_payload = b"mid_of:" + upstream
    (OUT_DIR / f"mid_{key}.bin").write_bytes(out_payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(out_payload)),
        metadata={"upstream_size": len(upstream)},
    )


defs = Definitions(assets=[raw_corner, mid_corner])
