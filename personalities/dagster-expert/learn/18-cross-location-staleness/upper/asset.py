"""Lesson 18 — upper code location.

Depends on `lib_lower/kit_summary` from a SEPARATE code location.
Cross-location wiring is done with `deps=[AssetKey([...])]`. The
upper asset reuses the same branch partition definition so the
default IdentityPartitionMapping wires partition X -> partition X
across the location boundary.

Key point: this `Definitions(assets=[...])` includes ONLY this
location's own assets. We do NOT declare an `AssetSpec` for the
cross-location upstream — that is the "Day-7 federation bug"
documented in `cross-location.md` cheatsheet.
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

BRANCHES = ["corner", "lvf", "em", "ht"]
branch_partitions = StaticPartitionsDefinition(BRANCHES)

LOWER_OUT = Path("/tmp/dagster-18-out/lower")
OUT_DIR = Path("/tmp/dagster-18-out/upper")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset(
    key_prefix=["lib_upper"],
    partitions_def=branch_partitions,
    deps=[AssetKey(["lib_lower", "kit_summary"])],
)
def signoff_report(context: AssetExecutionContext) -> MaterializeResult:
    """Reads upstream's file content (Style B), hashes it as own data_version.
    This is what makes staleness propagate across the code-location boundary."""
    key = context.partition_key
    upstream_path = LOWER_OUT / f"kit_{key}.bin"
    if not upstream_path.exists():
        raise FileNotFoundError(
            f"Upstream file missing: {upstream_path}. "
            f"Materialize lib_lower/kit_summary partition '{key}' first."
        )
    upstream_bytes = upstream_path.read_bytes()
    output = b"signoff:" + upstream_bytes
    (OUT_DIR / f"signoff_{key}.bin").write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"upstream_size": len(upstream_bytes)},
    )


defs = Definitions(assets=[signoff_report])
