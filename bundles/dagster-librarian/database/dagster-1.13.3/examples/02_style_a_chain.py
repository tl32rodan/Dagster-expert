"""02: Style A — function-arg deps, IOManager value flow,
automatic data_version propagation.

Three assets in a chain. Each downstream takes the upstream as
a function argument. Default IOManager pickles MaterializeResult.value
and feeds it to the next asset. Each asset hashes its OWN output
for data_version, so propagation is automatic.

Try: edit `payload` in raw_corner, then re-materialize raw → mid → final
in sequence; each step's data_version moves naturally.

Run:
    dagster dev -f 02_style_a_chain.py
"""

import hashlib

from dagster import DataVersion, Definitions, MaterializeResult, asset


def _digest(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


@asset
def raw_corner() -> MaterializeResult:
    payload = b"corner=ff_125c"
    return MaterializeResult(
        value=payload,                              # IOManager stores this
        data_version=DataVersion(_digest(payload)),
        metadata={"corner": "ff_125c", "size_bytes": len(payload)},
    )


@asset
def mid_corner(raw_corner: bytes) -> MaterializeResult:
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
