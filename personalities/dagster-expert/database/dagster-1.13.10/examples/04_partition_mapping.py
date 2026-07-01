"""`MultiToSingleDimensionPartitionMapping` — the Execution Fabric's
load-bearing mapping. Upstream is single-dim (`pvt`); downstream is
multi-dim (`[pvt, cell]`); the mapping projects on `pvt`.

Run: dagster definitions validate -m examples.04_partition_mapping
"""
import dagster as dg

pvt_pd = dg.StaticPartitionsDefinition(["tt_25", "ff_125"])
multi_pd = dg.MultiPartitionsDefinition({
    "pvt":  pvt_pd,
    "cell": dg.StaticPartitionsDefinition(["INV", "BUF"]),
})


@dg.asset(partitions_def=pvt_pd)
def template_tcl(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    pvt = context.partition_key
    return dg.MaterializeResult(
        data_version=dg.DataVersion(f"tcl_{pvt}"),
    )


@dg.asset(
    partitions_def=multi_pd,
    deps=[
        dg.AssetDep(
            dg.AssetKey("template_tcl"),
            partition_mapping=dg.MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="pvt"
            ),
        ),
    ],
)
def characterize(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    kd = context.partition_key.keys_by_dimension
    return dg.MaterializeResult(
        data_version=dg.DataVersion(f"char_{kd['pvt']}_{kd['cell']}"),
        metadata={"pvt": kd["pvt"], "cell": kd["cell"]},
    )


defs = dg.Definitions(assets=[template_tcl, characterize])
