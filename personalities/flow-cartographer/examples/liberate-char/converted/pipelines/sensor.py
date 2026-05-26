"""Sensor-driven incremental is the STANDARD_USAGE golden path (UI=observe,
CLI=execute, sensors=trigger). This sensor watches a drop dir for new netlist
files and requests the matching `characterize` partitions (one per PVT for the
dropped cell). Dedup via run_key; state persists in the cursor."""
import json
from pathlib import Path

from dagster import (
    AssetSelection, MultiPartitionKey, RunRequest, SensorEvaluationContext,
    SensorResult, SkipReason, define_asset_job, sensor,
)

from .assets import characterize
from .paths import DROP
from .spec.partitions import CELLS, PVTS

char_job = define_asset_job("char_job", selection=AssetSelection.assets(characterize))


@sensor(job=char_job, minimum_interval_seconds=30)
def netlist_drop_sensor(context: SensorEvaluationContext):
    seen = set(json.loads(context.cursor or "[]"))
    DROP.mkdir(parents=True, exist_ok=True)
    files = sorted(p.name for p in DROP.glob("*.sp"))
    new = [f for f in files if f not in seen]
    if not new:
        return SkipReason(f"no new netlist drops in {DROP}")

    requests = []
    for f in new:
        cell = Path(f).stem
        if cell not in CELLS:
            continue
        for pvt in PVTS:
            key = MultiPartitionKey({"pvt": pvt, "cell": cell})
            requests.append(RunRequest(run_key=f"{cell}-{pvt}-{f}", partition_key=key))
    seen.update(new)
    return SensorResult(run_requests=requests, cursor=json.dumps(sorted(seen)))
