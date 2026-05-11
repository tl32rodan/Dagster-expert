"""04: StaticPartitionsDefinition — one asset per corner.

Run:
    dagster dev -f 04_partitioned.py

UI: click `corner_summary`, partition strip shows 4 cells.
CLI: dagster asset materialize -f 04_partitioned.py \\
       --select corner_summary --partition ff_125c
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
    key = context.partition_key
    payload = f"summary_for:{key}".encode()
    digest = hashlib.sha256(payload).hexdigest()[:16]
    context.log.info(f"materializing partition {key}")
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"corner": key, "size_bytes": len(payload)},
    )


defs = Definitions(assets=[corner_summary])
