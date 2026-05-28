"""Config schema + validation rules."""
import pytest
import yaml

from char_dagster.config import CharConfig, load_config


def _write(tmp_path, overrides=None):
    """Write a minimal-valid config with optional shallow overrides."""
    base = {
        "project": {"name": "MYLIB", "version": "1.0", "lib_type": "typical"},
        "paths": {
            "work_dir": "/work/char",
            "generated_dir": "/work/char/generated",
            "template_dir": "./templates",
            "output_dir": "/work/char/out",
            "obf_cshrc": "/tools/obf/cshrc",
            "tool_version_script": "/tools/liberate/version.csh",
            "main_script": "char_trio_groupLPE_x_cworst_T_25c.tcl",
        },
        "tool_settings": {"liberate_version": "23.1.1"},
        "trio_groups": ["LPE_ssgnp_cworst_T_25c"],
        "pvt_corners": [
            {"name": "tt_25",  "volt": 0.9, "temp": 25, "active": True, "seed": True},
            {"name": "ff_125", "volt": 0.99, "temp": 125, "active": True, "seed": False},
        ],
        "cells": ["INV", "BUF"],
    }
    if overrides:
        for k, v in overrides.items():
            base[k] = v
    path = tmp_path / "char_config.yaml"
    path.write_text(yaml.safe_dump(base))
    return path


def test_loads_valid_config(tmp_path):
    cfg = load_config(_write(tmp_path))
    assert isinstance(cfg, CharConfig)
    assert cfg.project.name == "MYLIB"
    assert cfg.trio_groups == ["LPE_ssgnp_cworst_T_25c"]
    assert cfg.pvt_names == ["tt_25", "ff_125"]
    assert cfg.cells == ["INV", "BUF"]
    assert cfg.seed_pvt.name == "tt_25"


def test_refuses_two_seeds(tmp_path):
    bad = _write(tmp_path, {"pvt_corners": [
        {"name": "tt_25",  "volt": 0.9, "temp": 25, "active": True, "seed": True},
        {"name": "ff_125", "volt": 0.99, "temp": 125, "active": True, "seed": True},
    ]})
    with pytest.raises(ValueError, match="exactly one pvt"):
        load_config(bad)


def test_refuses_zero_seeds(tmp_path):
    bad = _write(tmp_path, {"pvt_corners": [
        {"name": "tt_25",  "volt": 0.9, "temp": 25, "active": True, "seed": False},
        {"name": "ff_125", "volt": 0.99, "temp": 125, "active": True, "seed": False},
    ]})
    with pytest.raises(ValueError, match="exactly one pvt"):
        load_config(bad)


def test_refuses_bad_trio_group(tmp_path):
    bad = _write(tmp_path, {"trio_groups": ["NOT_A_VALID_TRIO_GROUP"]})
    with pytest.raises(ValueError, match="does not match"):
        load_config(bad)


def test_refuses_empty_cells(tmp_path):
    bad = _write(tmp_path, {"cells": []})
    with pytest.raises(ValueError, match="cells"):
        load_config(bad)


def test_active_filter(tmp_path):
    cfg = load_config(_write(tmp_path, {"pvt_corners": [
        {"name": "tt_25",  "volt": 0.9, "temp": 25, "active": True, "seed": True},
        {"name": "ff_125", "volt": 0.99, "temp": 125, "active": False, "seed": False},
    ]}))
    assert cfg.pvt_names == ["tt_25"]
