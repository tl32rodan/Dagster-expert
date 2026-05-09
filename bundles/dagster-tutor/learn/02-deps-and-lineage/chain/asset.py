"""lab2: three assets in a chain — Style A (value-flow via IOManager).

Run with: dagster dev -m chain

Style A: function arg name = upstream asset key. Dagster's
default IOManager pickles each return value and feeds it to the
downstream as a function argument. Each asset hashes its OWN
output for `data_version`, so propagation is automatic — change
upstream → upstream output bytes change → its hash changes →
downstream sees different input → its hash changes → next
downstream stale.

This uses 100% public Dagster API: @asset, MaterializeResult
(with `value=` for output passthrough), DataVersion. No reach
into dagster._core.*

The TSMC EDA pattern (Style B: explicit deps + filesystem) is
documented in the README. The natural propagation there is
"downstream reads upstream's output FILE and hashes it" —
Dagster's metadata is not involved in the hash chain.
"""

import hashlib

from dagster import (
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset
def raw_corner() -> MaterializeResult:
    """Source asset. Edit `payload` to see staleness propagate
    downstream.
    """
    payload = b"corner=ff_125c_v1"
    return MaterializeResult(
        value=payload,                          # IOManager pickles + stores
        data_version=DataVersion(_digest(payload)),
        metadata={"corner": "ff_125c", "size_bytes": len(payload)},
    )


@asset
def mid_corner(raw_corner: bytes) -> MaterializeResult:
    """Style A: arg `raw_corner` matches upstream asset key.
    Dagster loads the upstream's stored value automatically.
    Our hash naturally depends on it.
    """
    output = b"mid_of:" + raw_corner
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
        metadata={"size_bytes": len(output)},
    )


@asset
def final_corner(mid_corner: bytes) -> MaterializeResult:
    output = b"final_of:" + mid_corner
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
        metadata={"size_bytes": len(output)},
    )


defs = Definitions(assets=[raw_corner, mid_corner, final_corner])
