"""17c · the constant-hash trap — staleness propagation silently breaks.

Three assets on the same partition definition:
  raw_corner -> mid_corner -> final_corner

`raw_corner` and `final_corner` are correct (Style B — hash own
output bytes).

`mid_corner` is **wrong on purpose**: its data_version is computed
from a constant string, ignoring upstream content. Run the demo
and watch the breakage:

  1. Re-materialize one partition of `raw_corner`.
  2. UI marks the matching partition of `mid_corner` stale.
  3. Re-materialize `mid_corner` for that partition.
  4. `mid_corner`'s data_version DOES NOT CHANGE between the two
     materializations -- because the hash was constant.
  5. UI therefore considers `final_corner` FRESH. The propagation
     stopped at `mid_corner`.

This is production-incident territory: edit upstream, re-run
intermediates, downstream silently does NOT update.

Run with: dagster dev -m trap
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

OUT_DIR = Path("/tmp/dagster-17c-out")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset(partitions_def=corner_partitions)
def raw_corner(context: AssetExecutionContext) -> MaterializeResult:
    """Correct. data_version actually reflects output content."""
    key = context.partition_key
    payload = f"raw__{key}__rev=1".encode()
    (OUT_DIR / f"raw_{key}.bin").write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"payload_preview": payload.decode()},
    )


@asset(
    partitions_def=corner_partitions,
    deps=[AssetKey("raw_corner")],
)
def mid_corner(context: AssetExecutionContext) -> MaterializeResult:
    """BROKEN ON PURPOSE.

    The hash is computed from a CONSTANT string. Even though the
    asset runs again when raw_corner is re-materialized, the
    data_version it stores is the same as before.

    Result: downstream of mid_corner never goes stale.
    """
    key = context.partition_key
    # Pretend we did real work — actually we just write a fixed file.
    output_path = OUT_DIR / f"mid_{key}.bin"
    output_path.write_bytes(b"this_never_changes")  # bug #1: ignored upstream
    constant_hash = _digest(f"mid_constant_for__{key}".encode())  # bug #2
    return MaterializeResult(
        data_version=DataVersion(constant_hash),
        metadata={"note": "constant hash — bug demo"},
    )


@asset(
    partitions_def=corner_partitions,
    deps=[AssetKey("mid_corner")],
)
def final_corner(context: AssetExecutionContext) -> MaterializeResult:
    """Correct. Reads its (broken) upstream's file bytes and hashes.

    Notice this asset is correctly written -- its own hash depends
    on the file it reads from disk. But because mid_corner always
    writes the same bytes, this asset's hash also never changes.
    The bug is the middle node, not this one.
    """
    key = context.partition_key
    upstream = (OUT_DIR / f"mid_{key}.bin").read_bytes()
    output = b"final_of:" + upstream
    (OUT_DIR / f"final_{key}.bin").write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
    )


defs = Definitions(assets=[raw_corner, mid_corner, final_corner])
