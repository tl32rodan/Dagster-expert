"""Layer-0 config loader + validation for char_dagster.

Single source of truth for everything templates need: project metadata,
trio_groups, pvt_corners, cells, paths. Validation is mechanical (no
judgment calls) so a weak implementing agent gets a hard refusal when
the YAML is wrong rather than a silent half-built flow.
"""
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml

TRIO_GROUP_RE = re.compile(r"^LPE_\w+_cworst_T_-?\d+c$")


@dataclass(frozen=True)
class Project:
    name: str
    version: str
    lib_type: str


@dataclass(frozen=True)
class Paths:
    work_dir: str
    generated_dir: str
    template_dir: str
    output_dir: str
    obf_cshrc: str
    tool_version_script: str
    main_script: str


@dataclass(frozen=True)
class ToolSettings:
    liberate_version: str


@dataclass(frozen=True)
class Pvt:
    name: str
    volt: float
    temp: int
    active: bool
    seed: bool


@dataclass(frozen=True)
class CharConfig:
    project: Project
    paths: Paths
    tool_settings: ToolSettings
    trio_groups: List[str]
    pvt_corners: List[Pvt]
    cells: List[str]

    @property
    def pvt_names(self) -> List[str]:
        return [p.name for p in self.pvt_corners if p.active]

    @property
    def seed_pvt(self) -> Pvt:
        return next(p for p in self.pvt_corners if p.seed)

    def pvt_by_name(self, name: str) -> Pvt:
        for p in self.pvt_corners:
            if p.name == name:
                return p
        raise KeyError(f"no pvt named {name!r}")


def _require(data: dict, key: str, where: str):
    if key not in data:
        raise ValueError(f"config: {where} missing key {key!r}")
    return data[key]


def load_config(path) -> CharConfig:
    data = yaml.safe_load(Path(path).read_text())

    proj_raw = _require(data, "project", "top-level")
    project = Project(
        name=_require(proj_raw, "name", "project"),
        version=str(_require(proj_raw, "version", "project")),
        lib_type=_require(proj_raw, "lib_type", "project"),
    )

    paths_raw = _require(data, "paths", "top-level")
    paths = Paths(**{
        k: _require(paths_raw, k, "paths")
        for k in ("work_dir", "generated_dir", "template_dir", "output_dir",
                 "obf_cshrc", "tool_version_script", "main_script")
    })

    ts_raw = _require(data, "tool_settings", "top-level")
    tool_settings = ToolSettings(
        liberate_version=str(_require(ts_raw, "liberate_version", "tool_settings")),
    )

    trio_groups = list(_require(data, "trio_groups", "top-level"))
    if not trio_groups:
        raise ValueError("config: 'trio_groups' is empty")
    for tg in trio_groups:
        if not TRIO_GROUP_RE.match(tg):
            raise ValueError(
                f"config: trio_group {tg!r} does not match {TRIO_GROUP_RE.pattern}"
            )

    pvts_raw = _require(data, "pvt_corners", "top-level")
    if not pvts_raw:
        raise ValueError("config: 'pvt_corners' is empty")
    pvts: List[Pvt] = []
    for p in pvts_raw:
        pvts.append(Pvt(
            name=_require(p, "name", "pvt_corners[]"),
            volt=float(_require(p, "volt", "pvt_corners[]")),
            temp=int(_require(p, "temp", "pvt_corners[]")),
            active=bool(p.get("active", True)),
            seed=bool(p.get("seed", False)),
        ))

    seeds = [p for p in pvts if p.seed]
    if len(seeds) != 1:
        raise ValueError(
            f"config: exactly one pvt must have 'seed: true' (got {len(seeds)})"
        )
    if not any(p.active for p in pvts):
        raise ValueError("config: at least one pvt must be active")

    cells = list(_require(data, "cells", "top-level"))
    if not cells:
        raise ValueError("config: 'cells' is empty")

    return CharConfig(
        project=project,
        paths=paths,
        tool_settings=tool_settings,
        trio_groups=trio_groups,
        pvt_corners=pvts,
        cells=cells,
    )
