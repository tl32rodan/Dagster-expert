"""Dagster Definitions entry point.

``workspace.yaml`` loads this module's ``defs``. The demo ships with a
single library ``lib_a``; multi-library scale-out follows lesson 11's
``key_prefix=[<lib>]`` pattern — flip the ``LIBRARIES`` list or run two
code locations.
"""
from __future__ import annotations

from dagster import Definitions

from .factory import build_all_assets
from .runners import PerlRunner, PythonRunner


LIBRARIES: list[str] = ["lib_a"]


defs = Definitions(
    assets=build_all_assets(LIBRARIES),
    resources={
        "perl_runner": PerlRunner(),
        "python_runner": PythonRunner(),
    },
)
