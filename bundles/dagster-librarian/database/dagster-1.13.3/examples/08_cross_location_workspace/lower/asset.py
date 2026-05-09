"""08 / lower — produces kit_summary at AssetKey(['lib_lower', 'kit_summary'])."""

import hashlib

from dagster import DataVersion, Definitions, MaterializeResult, asset


@asset(key_prefix=["lib_lower"])
def kit_summary() -> MaterializeResult:
    payload = b"kit_v1"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"kit_digest": digest},
    )


defs = Definitions(assets=[kit_summary])
