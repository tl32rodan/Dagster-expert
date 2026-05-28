"""Drop-folder sensor — kicks the LEAF asset (netlist_files) only;
AutomationCondition.eager() on every downstream asset cascades the rest.

Drop layout::

    <work_dir>/_drop/
        <trio_group>/
            <cell>.spi

The sensor emits one ``RunRequest`` per (trio_group, cell) seen for the
first time. Dedup is via the cursor; ``run_key`` provides the second-line
safety net.
"""
import json
from pathlib import Path

from dagster import (
    AssetSelection,
    MultiPartitionKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    define_asset_job,
    sensor,
)

from char_dagster.assets.source_generation import netlist_files
from char_dagster.config import load_config
from char_dagster.partitions import CELLS, TRIO_GROUPS, trio_x_cell
from char_dagster.paths import drop_dir

netlist_job = define_asset_job(
    "netlist_intake_job",
    selection=AssetSelection.assets(netlist_files),
    partitions_def=trio_x_cell,
)


@sensor(job=netlist_job, minimum_interval_seconds=30)
def cell_drop_sensor(context: SensorEvaluationContext):
    cfg = load_config(
        Path(__file__).resolve().parents[1] / "config" / "char_config.yaml"
    )
    seen = set(json.loads(context.cursor or "[]"))
    requests = []
    new_seen = set(seen)

    for tg in TRIO_GROUPS:
        sub = drop_dir(cfg, tg)
        sub.mkdir(parents=True, exist_ok=True)
        for spi in sorted(sub.glob("*.spi")):
            cell = spi.stem
            if cell not in CELLS:
                continue   # unknown cell — skip silently
            tag = f"{tg}|{cell}|{spi.name}"
            if tag in seen:
                continue
            requests.append(RunRequest(
                run_key=tag,
                partition_key=MultiPartitionKey({"trio_group": tg, "cell": cell}),
            ))
            new_seen.add(tag)

    if not requests:
        return SkipReason(
            f"no new .spi drops under {Path(cfg.paths.work_dir) / '_drop'}"
        )
    return SensorResult(
        run_requests=requests,
        cursor=json.dumps(sorted(new_seen)),
    )
