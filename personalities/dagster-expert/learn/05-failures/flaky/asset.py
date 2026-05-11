"""lab5: a partitioned asset that fails on one corner.

Run with: dagster dev -m flaky
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Backoff,
    DataVersion,
    Definitions,
    Jitter,
    MaterializeResult,
    RetryPolicy,
    StaticPartitionsDefinition,
    asset,
)

CORNERS = ["ff_125c", "tt_25c", "ss_m40c", "ss_125c"]
corner_partitions = StaticPartitionsDefinition(CORNERS)

# Rigging: ss_m40c fails every other invocation. We persist a
# counter to /tmp so re-runs alternate without code change.
COUNTER_PATH = Path("/tmp/flaky_ssm40c_counter")


@asset(
    partitions_def=corner_partitions,
    # Uncomment to see retry behavior:
    # retry_policy=RetryPolicy(
    #     max_retries=3,
    #     delay=2.0,
    #     backoff=Backoff.EXPONENTIAL,
    #     jitter=Jitter.PLUS_MINUS,
    # ),
)
def flaky_payload(context: AssetExecutionContext) -> MaterializeResult:
    key = context.partition_key

    if key == "ss_m40c":
        n = int(COUNTER_PATH.read_text()) if COUNTER_PATH.exists() else 0
        COUNTER_PATH.write_text(str(n + 1))
        if n % 2 == 0:
            raise RuntimeError(
                f"flaky failure on {key} (attempt {n + 1}); "
                f"will succeed on next run",
            )

    payload = f"flaky_ok:{key}".encode()
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"corner": key},
    )


defs = Definitions(assets=[flaky_payload])
