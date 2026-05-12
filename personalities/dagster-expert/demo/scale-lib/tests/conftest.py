"""Make ``pipelines`` importable when pytest runs from the demo root."""
import sys
from pathlib import Path

_DEMO_ROOT = Path(__file__).resolve().parents[1]
if str(_DEMO_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEMO_ROOT))
