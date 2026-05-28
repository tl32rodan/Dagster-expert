"""Unit tests for substitute_template — covers scalar, list, nested,
StrictUndefined-raises (the four shapes the asset code relies on)."""
import pytest
from jinja2 import UndefinedError

from char_dagster.utils import substitute_template


def test_scalar_substitution():
    tmpl = "v={{ voltage }} t={{ temperature }}"
    out = substitute_template(tmpl, {"voltage": 0.9, "temperature": 25})
    assert out == "v=0.9 t=25"


def test_list_for_loop():
    tmpl = "set cells {\n{% for c in cells %}  {{ c }}\n{% endfor %}}\n"
    out = substitute_template(tmpl, {"cells": ["INV", "BUF", "NAND2"]})
    assert out == "set cells {\n  INV\n  BUF\n  NAND2\n}\n"


def test_nested_dict_dot_access():
    tmpl = "{{ project.name }}-{{ pvt.name }} V={{ pvt.volt }}"
    out = substitute_template(tmpl, {
        "project": {"name": "MYLIB"},
        "pvt": {"name": "tt_25", "volt": 0.9},
    })
    assert out == "MYLIB-tt_25 V=0.9"


def test_conditional_block():
    tmpl = "{% if seed %}seed{% else %}nope{% endif %}"
    assert substitute_template(tmpl, {"seed": True}) == "seed"
    assert substitute_template(tmpl, {"seed": False}) == "nope"


def test_missing_variable_raises():
    """StrictUndefined: a missing placeholder must NOT silently render as
    empty — it must blow up so the asset never writes a half-rendered file."""
    with pytest.raises(UndefinedError):
        substitute_template("hello {{ missing_var }}", {"present": 1})


def test_dataclass_attribute_access():
    """Jinja's dot accessor also works on objects with attributes; the
    same template renders whether you pass a dict or a dataclass."""
    from dataclasses import dataclass

    @dataclass
    class Project:
        name: str

    out = substitute_template("p={{ project.name }}", {"project": Project("MYLIB")})
    assert out == "p=MYLIB"
