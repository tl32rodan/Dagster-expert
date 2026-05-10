"""lab1: one asset.

Run with: dagster dev -m hello
"""

from __future__ import annotations

import hashlib

from dagster import (
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)


@asset
def greeting() -> MaterializeResult:
    """The smallest possible asset: returns a fixed string.

    Edit `payload` and re-materialize to see data_version change.
    """
    payload = b"hello, dagster"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={
            "size_bytes": len(payload),
            "preview": payload.decode(),
        },
    )


defs = Definitions(assets=[greeting])
