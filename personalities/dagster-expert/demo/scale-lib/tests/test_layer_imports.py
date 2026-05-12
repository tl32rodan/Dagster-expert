"""Enforce the layer-import rule: ``spec/``, ``rules/``, and
``registry.py`` MUST NOT import dagster. The translator/factory layers
may.

This guards SOLID's DIP — Layer 2 depends on Layer 0/1 abstractions,
never on Dagster.
"""
import re
from pathlib import Path

import pytest

_DEMO_ROOT = Path(__file__).resolve().parents[1]
_PIPELINES = _DEMO_ROOT / "pipelines"

_FORBIDDEN_PATHS = [
    _PIPELINES / "spec",
    _PIPELINES / "rules",
    _PIPELINES / "registry.py",
]

_DAGSTER_IMPORT = re.compile(r"^\s*(?:from dagster|import dagster)\b", re.MULTILINE)


def _python_files(p: Path):
    if p.is_file():
        yield p
    elif p.is_dir():
        yield from sorted(p.rglob("*.py"))


@pytest.mark.parametrize("path", _FORBIDDEN_PATHS, ids=str)
def test_no_dagster_import(path: Path):
    offenders = []
    for f in _python_files(path):
        text = f.read_text()
        if _DAGSTER_IMPORT.search(text):
            offenders.append(str(f))
    assert not offenders, (
        f"Dagster import found in pure-data layer files: {offenders}. "
        "Move Dagster API usage into translator.py or factory.py."
    )


def test_translator_does_import_dagster():
    """Sanity: the boundary file SHOULD import dagster."""
    t = (_PIPELINES / "translator.py").read_text()
    assert _DAGSTER_IMPORT.search(t)


def test_factory_does_import_dagster():
    t = (_PIPELINES / "factory.py").read_text()
    assert _DAGSTER_IMPORT.search(t)
