"""Cascade trigger (Mode A — AutomationCondition.eager()) — schema parsing,
builder wiring, and generator's sensor-skip behavior.

End-to-end cascade is covered by scripts/run_demo.py (live daemon run).
"""
import dagster as dg
import pytest

from framework.assets.builder import build_asset
from framework.generator import build_definitions
from framework.spec.schema import FlowSpec
from framework.versioning.base import resolve_version

_BASE = {
    "version": 1,
    "flow_name": "cascade-toy",
    "dimensions": {"d": {"type": "static", "values": ["a"]}},
    "assets": [
        {"name": "root", "kind": "entry", "partitioned_by": []},
        {"name": "leaf", "kind": "compute",
         "script": "framework.versioning.base:content_hash_version",
         "partitioned_by": ["d"],
         "depends_on": [{"asset": "root", "mapping": "all"}]},
    ],
}


def _spec(defaults=None, leaf_trigger=None):
    data = {**_BASE}
    if defaults:
        data["defaults"] = defaults
    if leaf_trigger:
        data["assets"] = list(_BASE["assets"])
        data["assets"][1] = {**data["assets"][1], "trigger": leaf_trigger}
    return FlowSpec.model_validate(data)


def test_trigger_defaults_to_reconciliation_for_backcompat():
    spec = _spec()
    assert spec.effective_trigger(spec.assets[1]) == "reconciliation"


def test_trigger_automation_via_defaults():
    spec = _spec(defaults={"trigger": "automation"})
    assert spec.effective_trigger(spec.assets[0]) == "automation"
    assert spec.effective_trigger(spec.assets[1]) == "automation"


def test_trigger_per_asset_override():
    spec = _spec(defaults={"trigger": "reconciliation"}, leaf_trigger="automation")
    assert spec.effective_trigger(spec.assets[0]) == "reconciliation"
    assert spec.effective_trigger(spec.assets[1]) == "automation"


def test_trigger_invalid_rejected():
    bad = {**_BASE, "defaults": {"trigger": "nope"}}
    with pytest.raises(Exception):  # Pydantic ValidationError
        FlowSpec.model_validate(bad)


def test_builder_does_not_attach_automation_condition_either_way():
    """L15 outcome: framework never uses AutomationCondition.eager(). Both
    triggers rely on framework-built sensors (reconcile for reconciliation;
    cascade for automation). The asset itself stays vanilla."""
    for trig in ("automation", "reconciliation"):
        spec = _spec(defaults={"trigger": trig})
        leaf = build_asset(spec.assets[1], spec, resolve_version("content_hash"))
        leaf_spec = next(s for s in leaf.specs if s.key.to_user_string() == "leaf")
        assert leaf_spec.automation_condition is None, \
            f"trigger={trig}: should NOT have automation_condition attached"


def test_generator_replaces_reconcile_with_cascade_under_automation(tmp_path):
    """Under trigger=automation, framework swaps the per-compute reconcile
    sensor for ONE per-flow cascade sensor (+ matching cascade job).
    No per-asset _reconcile_sensor or _job should appear."""
    import yaml
    flow_dir = tmp_path / "flow_cas"
    flow_dir.mkdir()
    (flow_dir / "__init__.py").write_text("")
    spec_yaml = {
        **_BASE,
        "flow_name": "flow_cas",
        "defaults": {"trigger": "automation", "version": "content_hash", "dispatch": "local"},
    }
    spec_yaml["assets"][1] = {**spec_yaml["assets"][1],
                                "script": "framework.versioning.base:content_hash_version"}
    (flow_dir / "spec.yaml").write_text(yaml.safe_dump(spec_yaml))

    defs = build_definitions(str(tmp_path))
    repo = defs.get_repository_def()

    reconcile_sensors = [s.name for s in repo.sensor_defs if s.name.endswith("_reconcile_sensor")]
    assert reconcile_sensors == [], \
        f"automation trigger should not have per-compute reconcile sensors; got {reconcile_sensors}"

    cascade_sensors = [s.name for s in repo.sensor_defs if s.name.endswith("_cascade_sensor")]
    assert cascade_sensors == ["flow_cas_cascade_sensor"], \
        f"automation trigger should add exactly one cascade sensor per flow; got {cascade_sensors}"

    # One cascade job per non-entry asset (1.13.3 requires single partition
    # shape per asset_job — see LESSONS.md L16)
    cascade_jobs = sorted(j.name for j in repo.get_all_jobs() if "__cas_job" in j.name)
    assert cascade_jobs == ["flow_cas__leaf__cas_job"], \
        f"expected one cas_job per non-entry asset; got {cascade_jobs}"


def test_generator_builds_reconcile_sensor_under_reconciliation(tmp_path):
    import yaml
    flow_dir = tmp_path / "flow_rec"
    flow_dir.mkdir()
    (flow_dir / "__init__.py").write_text("")
    spec_yaml = {**_BASE, "flow_name": "flow_rec"}  # defaults trigger=reconciliation
    spec_yaml["assets"][1] = {**spec_yaml["assets"][1],
                                "script": "framework.versioning.base:content_hash_version"}
    (flow_dir / "spec.yaml").write_text(yaml.safe_dump(spec_yaml))

    defs = build_definitions(str(tmp_path))
    repo = defs.get_repository_def()
    fw_sensors = [s.name for s in repo.sensor_defs if s.name.endswith("_reconcile_sensor")]
    assert "leaf_reconcile_sensor" in fw_sensors
