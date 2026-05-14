"""17b · StaticPartitionMapping — different partition sets on each side.

Upstream and downstream have DIFFERENT partition definitions.
We declare which upstream key maps to which downstream key via
`StaticPartitionMapping`. This is the production pattern used by
`demo/scale-lib/` to wire branch -> branch (e.g.
`corner -> {lvf, em, ht}`).

Two scenarios in one module:

1. **Fan-out (1 -> N)**: `root_branch` has 1 partition (`root`);
   `leaf_branches` has 3 (`lvf`, `em`, `ht`); the *same* root
   feeds all 3 leaves. Re-materializing root flips ALL 3 leaves
   stale.

2. **Routing (N -> M, sparse)**: `upstream_branches` has 4
   (`ff`, `tt`, `ss`, `sf`); `downstream_branches` has 2
   (`fast_group`, `slow_group`). Mapping:
     ff -> fast_group, sf -> fast_group
     tt -> slow_group, ss -> slow_group
   Re-materializing `ff` only flips `fast_group` stale (not
   `slow_group`).

Run with: dagster dev -m staticmap
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)

OUT_DIR = Path("/tmp/dagster-17b-out")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


# ── Scenario 1: fan-out (1 -> N) ───────────────────────────────────────

root_partition = StaticPartitionsDefinition(["root"])
leaf_partitions = StaticPartitionsDefinition(["lvf", "em", "ht"])


@asset(partitions_def=root_partition)
def root_branch(context: AssetExecutionContext) -> MaterializeResult:
    payload = b"root_payload__rev=1"
    out = OUT_DIR / "root.bin"
    out.write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"path": str(out)},
    )


# Map every leaf partition back to the single "root" partition.
ROOT_TO_LEAF = StaticPartitionMapping(
    {"root": ["lvf", "em", "ht"]},
)


@asset(
    partitions_def=leaf_partitions,
    ins={
        "root_branch": AssetIn(
            key=AssetKey("root_branch"),
            partition_mapping=ROOT_TO_LEAF,
        ),
    },
)
def leaf_branches(
    context: AssetExecutionContext,
    root_branch: bytes,  # IOManager hands us the upstream value
) -> MaterializeResult:
    key = context.partition_key
    output = f"leaf__{key}__from:".encode() + root_branch
    (OUT_DIR / f"leaf_{key}.bin").write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"upstream_size": len(root_branch)},
    )


# ── Scenario 2: routing (N -> M, sparse) ───────────────────────────────

upstream_partitions = StaticPartitionsDefinition(["ff", "tt", "ss", "sf"])
group_partitions = StaticPartitionsDefinition(["fast_group", "slow_group"])


@asset(partitions_def=upstream_partitions)
def upstream_branches(context: AssetExecutionContext) -> MaterializeResult:
    key = context.partition_key
    payload = f"upstream__{key}__rev=1".encode()
    out = OUT_DIR / f"up_{key}.bin"
    out.write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"path": str(out)},
    )


# Sparse routing: each downstream group consumes a specific subset
# of upstream partitions.
UP_TO_GROUP = StaticPartitionMapping(
    {
        "ff": ["fast_group"],
        "sf": ["fast_group"],
        "tt": ["slow_group"],
        "ss": ["slow_group"],
    },
)


@asset(
    partitions_def=group_partitions,
    ins={
        "upstream_branches": AssetIn(
            key=AssetKey("upstream_branches"),
            partition_mapping=UP_TO_GROUP,
        ),
    },
)
def downstream_groups(
    context: AssetExecutionContext,
    upstream_branches: dict[str, bytes],  # mapping arg = dict of partition_key -> value
) -> MaterializeResult:
    key = context.partition_key
    # upstream_branches is a dict {upstream_partition_key: bytes}, one entry
    # per upstream partition mapped to this downstream partition.
    parts = sorted(upstream_branches.items())
    combined = b"||".join(f"{k}=".encode() + v for k, v in parts)
    output = f"group__{key}__from:".encode() + combined
    (OUT_DIR / f"group_{key}.bin").write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={
            "upstream_keys": ",".join(k for k, _ in parts),
            "upstream_count": len(parts),
        },
    )


defs = Definitions(
    assets=[root_branch, leaf_branches, upstream_branches, downstream_groups],
)
