"""lab3: one asset partitioned by corner.

Run with: dagster dev -m by_corner
"""

import hashlib

from dagster import (
    AssetExecutionContext,
    DataVersion,
    Definitions,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)

CORNERS = ["ff_125c", "tt_25c", "ss_m40c", "ss_125c"]
corner_partitions = StaticPartitionsDefinition(CORNERS)


@asset(partitions_def=corner_partitions)
def corner_summary(context: AssetExecutionContext) -> MaterializeResult:
    """One asset, four partitions. Each partition is a separate
    materialization with its own data version.
    """
    key = context.partition_key
    payload = f"NEW_summary_for:{key}".encode()
    digest = hashlib.sha256(payload).hexdigest()[:16]
    context.log.info(f"materializing partition {key}")
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={
            "corner": key,
            "size_bytes": len(payload),
        },
    )


defs = Definitions(assets=[corner_summary])
