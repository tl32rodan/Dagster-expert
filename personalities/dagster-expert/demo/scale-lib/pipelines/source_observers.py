"""Observable source assets — sources outside Dagster that Tier 1 watches
for change events. Change in source → staleness propagates.

Per the plan §B (接縫 B), the PVT manifest and cell list are sources;
both are observed via ``@observable_source_asset``.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from dagster import DataVersion, observable_source_asset


_DEMO_ROOT = Path(__file__).resolve().parents[1]
_PVT_MANIFEST = _DEMO_ROOT / "config" / "pvt_manifest.yaml"
_CELL_LIST = _DEMO_ROOT / "config" / "cells.json"


def _digest(path: Path) -> str:
    if not path.exists():
        return "missing"
    return hashlib.sha256(path.read_bytes()).hexdigest()[:32]


@observable_source_asset(name="pvt_manifest", group_name="sources")
def pvt_manifest_source():
    """The PVT spec list (script-internal Tier-2 input). Hashed on each
    observation run; downstream steps that read PVT see staleness on
    spec change.
    """
    return DataVersion(_digest(_PVT_MANIFEST))


@observable_source_asset(name="cell_list", group_name="sources")
def cell_list_source():
    """Per-library cell list (script-internal Tier-2 input)."""
    return DataVersion(_digest(_CELL_LIST))


SOURCE_ASSETS = [pvt_manifest_source, cell_list_source]
