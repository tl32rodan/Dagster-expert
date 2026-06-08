"""definitions.py — the code-location entry point Dagster loads via
workspace.yaml. You should NOT edit this; the framework's
build_definitions reads spec.yaml + script.py and produces the Dagster
objects.
"""
from pathlib import Path

from framework.generator import build_definitions

# flows/_template/  -> we want flows/ as the search root
_FLOW_DIR = Path(__file__).resolve().parent
_FLOWS_ROOT = _FLOW_DIR.parent

defs = build_definitions(str(_FLOWS_ROOT))
