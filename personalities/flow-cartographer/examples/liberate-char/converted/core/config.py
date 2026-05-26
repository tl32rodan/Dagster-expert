"""Layer-0 config loader + validation (the single source of truth that
replaces the hardcoded, duplicated source files in flow-src/)."""
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Config:
    flow_name: str
    pvts: dict      # pvt -> {process, voltage, temperature}
    cells: dict     # cell -> {pins}
    sections: list  # [int, ...]

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
    if not data["pvts"]:
        raise ValueError("config: 'pvts' is empty")
    if not data["cells"]:
        raise ValueError("config: 'cells' is empty")
    for pvt, params in data["pvts"].items():
        for field in ("process", "voltage", "temperature"):
            if field not in params:
                raise ValueError(f"config: pvt '{pvt}' missing '{field}'")
    for cell, params in data["cells"].items():
        if "pins" not in params:
            raise ValueError(f"config: cell '{cell}' missing 'pins'")
    return Config(
        flow_name=data["flow_name"],
        pvts=data["pvts"],
        cells=data["cells"],
        sections=list(data["sections"]),
    )
