"""08 / upper — depends on lib_lower's kit_summary.

CORRECT (Day-7 fix): only this code location's own asset goes
into Definitions(). The upstream key is referenced via deps=
only — never AssetSpec'd into Definitions(assets=[...]).

WRONG pattern that produces "Error loading base asset job":

    from dagster import AssetSpec
    external = AssetSpec(key=AssetKey(["lib_lower", "kit_summary"]))
    defs = Definitions(assets=[external, signoff_report])  # ← bug

Drop the AssetSpec; use only deps=[AssetKey([...])].
"""

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
    deps=[AssetKey(["lib_lower", "kit_summary"])],   # cross-loc dep
)
def signoff_report() -> MaterializeResult:
    payload = b"signoff_v1"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"signoff_digest": digest},
    )


defs = Definitions(assets=[signoff_report])         # only own asset
