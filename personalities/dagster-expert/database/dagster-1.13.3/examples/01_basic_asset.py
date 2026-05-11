"""01: smallest possible @asset.

Run:
    dagster dev -f 01_basic_asset.py

Validate:
    dagster definitions validate -f 01_basic_asset.py
"""

import hashlib

from dagster import DataVersion, Definitions, MaterializeResult, asset


@asset
def greeting() -> MaterializeResult:
    payload = b"hello, dagster"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"size_bytes": len(payload), "preview": payload.decode()},
    )


defs = Definitions(assets=[greeting])
