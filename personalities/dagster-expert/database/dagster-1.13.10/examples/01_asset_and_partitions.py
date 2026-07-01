"""Minimal multi-partition asset with content_hash data_version.

Run: dagster definitions validate -m examples.01_asset_and_partitions
"""
import hashlib

import dagster as dg

pd = dg.MultiPartitionsDefinition({
    "pvt":  dg.StaticPartitionsDefinition(["tt_25", "ff_125"]),
    "cell": dg.StaticPartitionsDefinition(["INV", "BUF"]),
})


@dg.asset(partitions_def=pd)
def my_artifact(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    blob = f"{kd['pvt']}_{kd['cell']}".encode()
    digest = hashlib.sha256(blob).hexdigest()[:16]
    return dg.MaterializeResult(
        data_version=dg.DataVersion(digest),
        metadata={"pvt": kd["pvt"], "cell": kd["cell"]},
    )


defs = dg.Definitions(assets=[my_artifact])
