"""M1 schema validation — the good pass, the bad fail."""
import pytest
from pydantic import ValidationError

from framework.spec.schema import FlowSpec

GOOD = {
    "version": 1,
    "flow_name": "liberate_char",
    "dimensions": {
        "pvt": {"type": "static", "values": ["tt_25", "ff_125", "ss_m40"]},
        "cell": {"type": "static", "values": ["INV", "BUF", "NAND2"]},
    },
    "defaults": {"version": "content_hash"},
    "lsf": {"default": {"queue": "normal", "cores": 4, "mem_mb": 4096, "walltime": "24:00"}},
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
    assert spec.flow_name == "liberate_char"
    assert spec.effective_version(spec.assets[0]) == "content_hash"
    assert spec.effective_lsf(spec.assets[1]).cores == 4


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


def test_per_asset_lsf_overrides_default():
    spec_data = {**GOOD, "assets": [
        *GOOD["assets"],
        {"name": "characterize2", "kind": "compute",
         "script": "m:f", "partitioned_by": ["pvt"],
         "depends_on": [{"asset": "template_tcl"}],
         "lsf": {"queue": "premium", "cores": 16, "mem_mb": 32768, "walltime": "12:00"}},
    ]}
    spec = FlowSpec.model_validate(spec_data)
    assert spec.effective_lsf(spec.assets[2]).queue == "premium"
    assert spec.effective_lsf(spec.assets[1]).queue == "normal"


def test_generator_without_script_rejected():
    bad = {**GOOD, "assets": [{"name": "x", "kind": "generator"}]}
    with pytest.raises(ValidationError, match="requires `script`"):
        FlowSpec.model_validate(bad)


def test_entry_with_script_rejected():
    bad = {**GOOD, "assets": [{"name": "x", "kind": "entry", "script": "m:f"}]}
    with pytest.raises(ValidationError, match="must not have `script`"):
        FlowSpec.model_validate(bad)
