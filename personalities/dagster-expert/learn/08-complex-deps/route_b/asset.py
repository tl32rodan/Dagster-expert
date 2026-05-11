"""lab8 route_b: MultiPartitionsDefinition for the same matrix.

Three assets total (step0, step5, step6); the partition space
encodes the combos. Invalid combos are guarded inside the asset
body — they materialize as failure rather than being absent.

NOTE: Dagster 1.13.3's MultiPartitionsDefinition supports at
most 2 dimensions. We model (corner) × (em_ht) — the original
3D matrix collapsed to 2D by joining em + ht into a single key
(e.g. "em_lo__ht_low"). For >2 dims you must either:
  - Encode extra dims as a single composite partition key (this
    file's approach), or
  - Drop down to Route A (concrete assets), or
  - Build per-dim partitions externally and use dynamic
    partitions to register valid combos at runtime.
This is itself a strong argument for Route A when N is small
and dimensions exceed 2.
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

CORNERS = ["ff", "tt", "ss", "sf"]
EM_HT_PAIRS = [
    "em_lo__ht_low",
    "em_lo__ht_mid",
    "em_lo__ht_high",
    "em_mid__ht_low",
    "em_mid__ht_mid",
    "em_mid__ht_high",
    "em_hi__ht_low",
    "em_hi__ht_mid",
    "em_hi__ht_high",
]

# The same sparse matrix as route_a, expressed as (corner, em_ht_pair)
VALID_COMBOS = {
    ("ff", "em_lo__ht_low"),
    ("ff", "em_hi__ht_high"),
    ("tt", "em_mid__ht_mid"),
    ("ss", "em_lo__ht_low"),
    ("ss", "em_mid__ht_mid"),
    ("ss", "em_hi__ht_high"),
    ("sf", "em_mid__ht_mid"),
}

corner_partitions = StaticPartitionsDefinition(CORNERS)
step5_partitions = MultiPartitionsDefinition({
    "corner": StaticPartitionsDefinition(CORNERS),
    "em_ht":  StaticPartitionsDefinition(EM_HT_PAIRS),
})


def _payload(label: str) -> MaterializeResult:
    data = label.encode()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(data).hexdigest()[:16]),
        metadata={"label": label},
    )


@asset(partitions_def=corner_partitions)
def step0(context: AssetExecutionContext) -> MaterializeResult:
    return _payload(f"step0:{context.partition_key}")


@asset(partitions_def=step5_partitions)
def step5(context: AssetExecutionContext) -> MaterializeResult:
    keys = context.partition_key.keys_by_dimension
    combo = (keys["corner"], keys["em_ht"])
    if combo not in VALID_COMBOS:
        raise ValueError(f"step5: invalid combo {combo}; skipping")
    return _payload(f"step5:{combo[0]}:{combo[1]}")


@asset(partitions_def=corner_partitions)
def step6(context: AssetExecutionContext) -> MaterializeResult:
    """Conceptually fans in over step0 + all step5 partitions of
    this corner. With MultiPartitions you'd write a custom
    PartitionMapping to express that; for the lab we just signal
    the dependency conceptually and trust the operator.
    """
    return _payload(f"step6:{context.partition_key}")


defs = Definitions(assets=[step0, step5, step6])
