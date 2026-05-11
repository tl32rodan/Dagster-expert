"""07: RetryPolicy with exponential backoff + jitter.

Asset is rigged to fail every other run on partition 'ss_m40c'.
With retry_policy in place, it'll succeed within 3 retries.

Run:
    dagster dev -f 07_retry_policy.py
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
COUNTER_PATH = Path("/tmp/dagster-librarian-retry-counter")


@asset(
    partitions_def=StaticPartitionsDefinition(CORNERS),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def flaky_payload(context: AssetExecutionContext) -> MaterializeResult:
    key = context.partition_key

    if key == "ss_m40c":
        n = int(COUNTER_PATH.read_text()) if COUNTER_PATH.exists() else 0
        COUNTER_PATH.write_text(str(n + 1))
        if n % 2 == 0:
            raise RuntimeError(
                f"flaky failure on {key} (attempt {n + 1}); "
                f"will succeed next try",
            )

    payload = f"flaky_ok:{key}".encode()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()[:16]),
        metadata={"corner": key},
    )


defs = Definitions(assets=[flaky_payload])
