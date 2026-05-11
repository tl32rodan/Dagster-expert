"""6a · slow asset for cancel-during-run testing.

The asset sleeps for 30 seconds in 1-second increments so it stays
responsive to Dagster's cancel signal (SIGTERM → KeyboardInterrupt).
"""

from __future__ import annotations

import hashlib
import time

from dagster import (
    AssetExecutionContext,
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)


@asset
def slow_payload(context):
    """Sleeps for 30 seconds, logs every second.

    Cancel this mid-run via the UI's Terminate button.
    """
    duration = 30
    for i in range(duration):
        context.log.info(f"slow tick {i + 1}/{duration}")
        time.sleep(1)

    payload = b"slow done"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"slept_seconds": duration},
    )


defs = Definitions(assets=[slow_payload])
