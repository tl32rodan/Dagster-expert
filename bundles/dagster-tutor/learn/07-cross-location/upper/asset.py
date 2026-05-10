"""lab7: lib_upper — depends on lib_lower/kit_summary.

CORRECT pattern (per Day-7 fix): only this code location's own
asset goes into Definitions. The upstream key is referenced via
deps=[AssetKey([...])] only — never as an AssetSpec in the
assets list.
"""

from __future__ import annotations

import hashlib

from dagster import (
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)


@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],
)
def signoff_report() -> MaterializeResult:
    payload = b"signoff_v1"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"signoff_digest": digest},
    )


defs = Definitions(assets=[signoff_report])
