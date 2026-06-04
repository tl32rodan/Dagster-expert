"""M1 schema validation — the good pass, the bad fail."""
import pytest
from pydantic import ValidationError

from framework.spec.schema import FlowSpec

GOOD = {
    "version": 1,
    "flow_name": "liberate-char",
    "dimensions": {
        "pvt": {"type": "static", "values": ["tt_25", "ff_125", "ss_m40"]},
        "cell": {"type": "static", "values": ["INV", "BUF", "NAND2"]},
    },
    "defaults": {"dispatch": "local", "version": "content_hash"},
    "assets": [
        {"name": "template_tcl", "kind": "generator",
         "script": "framework.versioning.base:content_hash_version",
         "partitioned_by": ["pvt"]},
        {"name": "characterize", "kind": "compute",
         "script": "framework.versioning.base:content_hash_version",
         "partitioned_by": ["pvt", "cell"],
         "depends_on": [{"asset": "template_tcl", "mapping": {"pvt": "identity"}}]},
    ],
}


def test_good_spec_loads():
    spec = FlowSpec.model_validate(GOOD)
    assert spec.flow_name == "liberate-char"
    assert spec.effective_dispatch(spec.assets[1]) == "local"
    assert spec.effective_version(spec.assets[0]) == "content_hash"


def test_partitioned_by_undefined_dimension_rejected():
    bad = {**GOOD, "assets": [
        {"name": "x", "kind": "generator", "script": "m:f", "partitioned_by": ["nope"]},
    ]}
    with pytest.raises(ValidationError, match="undefined dimension"):
        FlowSpec.model_validate(bad)


def test_depends_on_unknown_asset_rejected():
    bad = {**GOOD, "assets": [
        {"name": "x", "kind": "generator", "script": "m:f",
         "depends_on": [{"asset": "ghost"}]},
    ]}
    with pytest.raises(ValidationError, match="unknown asset"):
        FlowSpec.model_validate(bad)


def test_compute_lsf_without_resource_rejected():
    bad = {
        "version": 1, "flow_name": "f",
        "dimensions": {"pvt": {"type": "static", "values": ["a"]}},
        "defaults": {"dispatch": "lsf"},
        "assets": [
            {"name": "c", "kind": "compute", "script": "m:f", "partitioned_by": ["pvt"]},
        ],
    }
    with pytest.raises(ValidationError, match="dispatch=lsf but no lsf resource"):
        FlowSpec.model_validate(bad)


def test_compute_lsf_with_default_resource_ok():
    ok = {
        "version": 1, "flow_name": "f",
        "dimensions": {"pvt": {"type": "static", "values": ["a"]}},
        "defaults": {"dispatch": "lsf"},
        "lsf": {"default": {"queue": "normal", "cores": 4, "mem_mb": 4096, "walltime": "24:00"}},
        "assets": [
            {"name": "c", "kind": "compute", "script": "m:f", "partitioned_by": ["pvt"]},
        ],
    }
    spec = FlowSpec.model_validate(ok)
    assert spec.effective_lsf(spec.assets[0]).cores == 4


def test_generator_without_script_rejected():
    bad = {**GOOD, "assets": [{"name": "x", "kind": "generator"}]}
    with pytest.raises(ValidationError, match="requires `script`"):
        FlowSpec.model_validate(bad)
