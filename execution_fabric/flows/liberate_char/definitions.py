"""Code location entry point: framework generates Definitions from spec.yaml."""
from pathlib import Path

from framework.generator import build_definitions

_FLOW_DIR = Path(__file__).resolve().parent          # flows/liberate_char
_FLOWS_ROOT = _FLOW_DIR.parent                        # flows/

defs = build_definitions(str(_FLOWS_ROOT))
