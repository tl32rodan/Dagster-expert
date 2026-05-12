"""Lesson 17 — observer-mode Tier 1.

The observer-mode adoption pattern: Tier 1 (Dagster) watches the
existing AP (still in production, unchanged) via touch files and
records each AP step completion as a Dagster materialization.
Zero risk; backward-compatible; UI immediately surfaces an
execution record (TSMC AP painpoint #3 solved without touching AP).

Architecture:

    AP (unchanged)                       Tier 1 (Dagster, observer-mode)
    ──────────────                       ─────────────────────────────────
    runs my_step.pl
       ↓ writes output to disk            (a) @observable_source_asset
       ↓ touches /tmp/mock-ap/step1/...      polls every N seconds,
                                              hashes touch-file mtime
                                          (b) sensor watches the source
                                              asset; new observation
                                              triggers Tier 1 asset materialize
                                          (c) Tier 1 asset computes folder
                                              digest from the AP output
                                              (read-only) and emits
                                              MaterializeResult

Result: AP keeps writing its old touch files. Tier 1 keeps a run
log + folder digest history without ever scheduling anything.

After ~1 week of observer-mode in production, you can:
- Diff digests across runs to find which steps actually changed
- Identify steps with noisy digests (need exclusion rules)
- Build trust before letting Tier 1 actively schedule

Run: dagster dev -m pipelines
Smoke: python -m _smoke
"""

import hashlib
import json
import time
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AutoMaterializePolicy,
    DataVersion,
    Definitions,
    MaterializeResult,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset,
    define_asset_job,
    observable_source_asset,
    sensor,
)


# Mock AP directories
AP_ROOT = Path("/tmp/dagster-17-mock-ap")
TIER1_RECORDS = Path("/tmp/dagster-17-tier1-records")
AP_ROOT.mkdir(parents=True, exist_ok=True)
TIER1_RECORDS.mkdir(parents=True, exist_ok=True)

AP_STEPS = ["step0", "step1", "step2"]


def _ap_touch_file(step: str) -> Path:
    return AP_ROOT / step / ".done"


def _ap_output_dir(step: str) -> Path:
    return AP_ROOT / step / "out"


def _digest_folder(p: Path) -> str:
    """Manifest hash. ~scale-lib's folder_digest, simplified."""
    if not p.exists():
        return "missing"
    entries = []
    for f in sorted(p.rglob("*")):
        if f.is_file():
            entries.append(f"{f.relative_to(p)}:{f.stat().st_size}:{int(f.stat().st_mtime)}")
    return hashlib.sha256("\n".join(entries).encode()).hexdigest()[:32]


# ── (a) Source assets — one per AP step. Observable on a tick. ────

def _make_ap_source(step: str):
    @observable_source_asset(
        name=f"ap_{step}_touch",
        group_name="ap_observed",
        description=(
            f"Observable source asset for AP {step}. The data_version "
            f"is the mtime of /tmp/dagster-17-mock-ap/{step}/.done. "
            f"When AP touches the file, the next observation tick "
            f"surfaces a new data_version and downstream Tier 1 assets "
            f"go stale."
        ),
    )
    def _src():
        touch = _ap_touch_file(step)
        if not touch.exists():
            return DataVersion("missing")
        # Use mtime as the version — newer touch = new version
        return DataVersion(str(int(touch.stat().st_mtime)))
    return _src


_ap_sources = [_make_ap_source(s) for s in AP_STEPS]


# ── (b) Tier-1 asset per AP step. Materializes when source moves. ──

def _make_tier1_recorder(step: str):
    source_key = AssetKey([f"ap_{step}_touch"])

    @asset(
        name=f"tier1_{step}_record",
        group_name="tier1_records",
        deps=[source_key],
        auto_materialize_policy=AutoMaterializePolicy.eager(),
        description=(
            f"Tier-1 record for AP {step}. Reads the AP output folder "
            f"(read-only), computes folder_digest, emits a "
            f"MaterializeResult. Hash of the folder is the data_version."
        ),
    )
    def _tier1(context: AssetExecutionContext) -> MaterializeResult:
        out_dir = _ap_output_dir(step)
        digest = _digest_folder(out_dir)
        record = TIER1_RECORDS / f"{step}__{int(time.time())}.json"
        record.write_text(json.dumps({
            "step": step,
            "ap_touch_mtime": (
                int(_ap_touch_file(step).stat().st_mtime)
                if _ap_touch_file(step).exists() else None
            ),
            "folder_digest": digest,
            "observed_at": int(time.time()),
            "file_count": (
                sum(1 for _ in out_dir.rglob("*") if _.is_file())
                if out_dir.exists() else 0
            ),
        }))
        return MaterializeResult(
            data_version=DataVersion(digest),
            metadata={
                "step": step,
                "ap_output_dir": str(out_dir),
                "tier1_record_path": str(record),
                "folder_digest": digest,
            },
        )
    return _tier1


_tier1_recorders = [_make_tier1_recorder(s) for s in AP_STEPS]


# ── (c) Optional: explicit sensor to force-rematerialize.
# In production with auto_materialize_policy.eager(), the daemon
# handles this automatically. We add a sensor here for the demo
# so a smoke run without the daemon can still trigger.

@sensor(
    name="ap_touch_sensor",
    asset_selection=AssetSelection.assets(*_tier1_recorders),
    minimum_interval_seconds=10,
    description=(
        "Explicit poll: check if any AP touch file moved since last "
        "tick; if so, request materialization of the corresponding "
        "tier1 record. Redundant when AutoMaterializePolicy.eager() "
        "is on, but useful for demos / smoke runs."
    ),
)
def ap_touch_sensor(context: SensorEvaluationContext):
    cursor = json.loads(context.cursor or "{}")
    requests = []
    new_cursor = dict(cursor)
    for step in AP_STEPS:
        touch = _ap_touch_file(step)
        if not touch.exists():
            continue
        mtime = int(touch.stat().st_mtime)
        last = cursor.get(step)
        if last is None or mtime > last:
            requests.append(RunRequest(
                run_key=f"{step}_{mtime}",
                asset_selection=[AssetKey([f"tier1_{step}_record"])],
                tags={"step": step, "ap_mtime": str(mtime)},
            ))
            new_cursor[step] = mtime
    if not requests:
        return SkipReason("no AP touch-file mtime change since last tick")
    return SensorResult(run_requests=requests, cursor=json.dumps(new_cursor))


defs = Definitions(
    assets=[*_ap_sources, *_tier1_recorders],
    sensors=[ap_touch_sensor],
)
