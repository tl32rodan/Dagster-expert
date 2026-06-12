"""Layer-0 config loader. Vendored from converted/core/config.py."""
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Config:
    flow_name: str
    pvts: dict
    cells: dict
    sections: list

    @property
    def pvt_keys(self) -> list:
        return list(self.pvts.keys())

    @property
    def cell_keys(self) -> list:
        return list(self.cells.keys())


def load_config(path) -> Config:
    data = yaml.safe_load(Path(path).read_text())
    for key in ("flow_name", "pvts", "cells", "sections"):
        if key not in data:
            raise ValueError(f"config missing top-level key: {key}")
    return Config(
        flow_name=data["flow_name"],
        pvts=data["pvts"],
        cells=data["cells"],
        sections=list(data["sections"]),
    )
