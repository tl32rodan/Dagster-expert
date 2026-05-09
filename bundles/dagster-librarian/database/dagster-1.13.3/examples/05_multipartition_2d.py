"""05: MultiPartitionsDefinition (2D — 1.13.3 limit).

Adding a 3rd dimension raises:
    DagsterInvalidInvocationError: only supports 2 partitions

For 3+ dims, collapse two into a composite key (this file's
em_ht), or drop to concrete-asset Route A pattern, or use
DynamicPartitionsDefinition.

Run:
    dagster dev -f 05_multipartition_2d.py
"""

import hashlib

from dagster import (
    AssetExecutionContext,
    DataVersion,
    Definitions,
    MaterializeResult,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)

CORNERS = ["ff", "tt", "ss"]
EM_HT_PAIRS = [
    "em_lo__ht_low",
    "em_lo__ht_high",
    "em_hi__ht_low",
    "em_hi__ht_high",
]

corner_em_ht = MultiPartitionsDefinition({
    "corner": StaticPartitionsDefinition(CORNERS),
    "em_ht":  StaticPartitionsDefinition(EM_HT_PAIRS),       # collapsed dim
})


@asset(partitions_def=corner_em_ht)
def per_corner_per_em_ht(context: AssetExecutionContext) -> MaterializeResult:
    keys = context.partition_key.keys_by_dimension
    label = f"{keys['corner']}:{keys['em_ht']}"
    payload = label.encode()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()[:16]),
        metadata={"label": label},
    )


defs = Definitions(assets=[per_corner_per_em_ht])
