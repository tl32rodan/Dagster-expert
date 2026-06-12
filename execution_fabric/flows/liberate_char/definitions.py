"""Code location entry point: framework generates Definitions from spec.yaml.

The bsub binary path can be overridden via $FABRIC_BSUB_BIN — useful for
local-sim where the mock bsub.py lives at a known absolute path. If
the env var is not set, defaults to "bsub" (PATH lookup), matching the
real-LSF case.
"""
import os
import sys
from pathlib import Path

from framework.generator import build_definitions

_FLOW_DIR = Path(__file__).resolve().parent          # flows/liberate_char
_FLOWS_ROOT = _FLOW_DIR.parent                        # flows/
_MOCK_BSUB = _FLOW_DIR / "_vendor" / "bin" / "bsub.py"

_bsub_bin = os.environ.get("FABRIC_BSUB_BIN") or (
    str(_MOCK_BSUB) if os.environ.get("FABRIC_USE_MOCK_BSUB") else "bsub"
)
_invoker = [sys.executable] if _bsub_bin.endswith(".py") else None

defs = build_definitions(
    str(_FLOWS_ROOT),
    bsub_bin=_bsub_bin,
    invoker=_invoker,
)
