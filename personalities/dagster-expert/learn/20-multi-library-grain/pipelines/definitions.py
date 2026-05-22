"""Dagster ``Definitions`` entry point loaded by ``workspace.yaml``.

Builds 2,100 assets (100 libraries × 21 steps) at module-import time.
Load time on a warm CPython process is ~5-15 s; the bulk is Dagster's
own asset-graph validation, not the pure-Python build. Once loaded,
materialization is independent per asset.
"""
from __future__ import annotations

from dagster import Definitions

from .factory import build_all_assets
from .libraries import LIBRARIES


defs = Definitions(
    assets=build_all_assets(LIBRARIES),
)
