"""03: Style B — explicit deps + downstream reads upstream's
output FILE. The natural EDA pattern.

Each asset writes its output to /tmp; downstream reads upstream's
file and hashes the actual bytes for its data_version. No Dagster
metadata is involved in the propagation chain.

Try: edit `payload` in raw_corner, materialize raw → mid → final
in sequence. final_corner's data_version moves because it ends
up hashing different bytes (via mid's file, which hashes raw's
file).

Run:
    dagster dev -f 03_style_b_filesystem.py
"""

import hashlib
from pathlib import Path

from dagster import (
    AssetKey,
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)

OUT_DIR = Path("/tmp/dagster-librarian-style-b")
OUT_DIR.mkdir(exist_ok=True)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset
def raw_corner() -> MaterializeResult:
    payload = b"corner=ff_125c"
    out = OUT_DIR / "raw_corner.bin"
    out.write_bytes(payload)
    return MaterializeResult(
        data_version=DataVersion(_digest(payload)),
        metadata={"path": str(out), "size_bytes": len(payload)},
    )


@asset(deps=[AssetKey("raw_corner")])
def mid_corner() -> MaterializeResult:
    upstream = (OUT_DIR / "raw_corner.bin").read_bytes()    # YOU read upstream
    output = b"mid_of:" + upstream                           # depends on it
    out = OUT_DIR / "mid_corner.bin"
    out.write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"path": str(out), "size_bytes": len(output)},
    )


@asset(deps=[AssetKey("mid_corner")])
def final_corner() -> MaterializeResult:
    upstream = (OUT_DIR / "mid_corner.bin").read_bytes()
    output = b"final_of:" + upstream
    out = OUT_DIR / "final_corner.bin"
    out.write_bytes(output)
    return MaterializeResult(
        data_version=DataVersion(_digest(output)),
        metadata={"path": str(out), "size_bytes": len(output)},
    )


defs = Definitions(assets=[raw_corner, mid_corner, final_corner])
