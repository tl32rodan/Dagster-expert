"""M1 — Pydantic spec schema (whitepaper §3.2).

Strict validation at load time: a malformed spec fails here, never at
runtime. kind is decoupled from dispatch (whitepaper inversion): the
same compute can dispatch=local (small scale) or dispatch=lsf (large
scale) without changing kind.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

Dispatch = Literal["local", "lsf"]
Kind = Literal["entry", "generator", "compute"]
Trigger = Literal["reconciliation", "automation"]
# - reconciliation: framework builds a reconcile sensor (desired - observed),
#   sensor ticks every 30s and dispatches missing partitions. Backfill-friendly,
#   no cascade — fits the original D1 model.
# - automation: framework attaches AutomationCondition.eager() to the asset;
#   AssetDaemon evaluates and cascades materializations as upstreams change.
#   Fits the "materialize root → cascade pipeline" verification pattern.


class DimensionSpec(BaseModel):
    type: Literal["static", "dynamic"]
    values: list[str] | None = None  # static: required
    source: str | None = None        # dynamic: required (resource key)

    @model_validator(mode="after")
    def _check(self) -> "DimensionSpec":
        if self.type == "static" and not self.values:
            raise ValueError("static dimension requires non-empty `values`")
        if self.type == "dynamic" and not self.source:
            raise ValueError("dynamic dimension requires `source`")
        return self


class DependencySpec(BaseModel):
    asset: str
    # mapping is an INTENT, not Python. The generator translates it into a
    # direction-correct PartitionMapping (framework/assets/mapping_builder.py).
    #   "all"  -> AllPartitionMapping
    #   "last" -> LastPartitionMapping
    #   "identity" / {dim: "identity"} -> identity or MultiToSingleDimension
    mapping: str | dict[str, str] = "identity"


class LSFResource(BaseModel):
    queue: str = "normal"
    cores: int = 4
    mem_mb: int = 4096
    walltime: str = "24:00"
    project: str | None = None


class AssetSpec(BaseModel):
    name: str
    kind: Kind
    script: str | None = None  # "module.path:callable"
    version: str | None = None  # base-version name or "module:callable"; None -> inherit defaults
    partitioned_by: list[str] = Field(default_factory=list)
    depends_on: list[DependencySpec] = Field(default_factory=list)
    dispatch: Dispatch | None = None  # None -> inherit defaults.dispatch
    trigger: Trigger | None = None    # None -> inherit defaults.trigger
    op_tags: dict[str, str] = Field(default_factory=dict)
    lsf: LSFResource | None = None

    @model_validator(mode="after")
    def _check(self) -> "AssetSpec":
        if self.kind in ("generator", "compute") and not self.script:
            raise ValueError(f"asset '{self.name}' ({self.kind}) requires `script`")
        if self.kind == "entry" and self.script:
            raise ValueError(f"entry asset '{self.name}' must not have `script`")
        return self


class FlowDefaults(BaseModel):
    dispatch: Dispatch = "local"
    version: str = "content_hash"
    trigger: Trigger = "reconciliation"  # back-compat: existing flows keep current behavior


class FlowSpec(BaseModel):
    version: int
    flow_name: str
    dimensions: dict[str, DimensionSpec] = Field(default_factory=dict)
    defaults: FlowDefaults = Field(default_factory=FlowDefaults)
    assets: list[AssetSpec]
    lsf: dict[str, LSFResource] = Field(default_factory=dict)  # {"default": LSFResource}

    # ---- resolved-view helpers (defaults applied) ----
    def effective_dispatch(self, a: AssetSpec) -> str:
        return a.dispatch or self.defaults.dispatch

    def effective_version(self, a: AssetSpec) -> str:
        return a.version or self.defaults.version

    def effective_lsf(self, a: AssetSpec) -> LSFResource | None:
        return a.lsf or self.lsf.get("default")

    def effective_trigger(self, a: AssetSpec) -> str:
        return a.trigger or self.defaults.trigger

    @model_validator(mode="after")
    def _check(self) -> "FlowSpec":
        names = {a.name for a in self.assets}
        for a in self.assets:
            # partitioned_by dims must be defined
            for d in a.partitioned_by:
                if d not in self.dimensions:
                    raise ValueError(
                        f"asset '{a.name}' partitioned_by undefined dimension '{d}'"
                    )
            # depends_on must reference assets in this spec
            for dep in a.depends_on:
                if dep.asset not in names:
                    raise ValueError(
                        f"asset '{a.name}' depends_on unknown asset '{dep.asset}'"
                    )
            # dispatch=lsf requires the four LSF fields somewhere (asset or default)
            if a.kind == "compute" and self.effective_dispatch(a) == "lsf":
                res = self.effective_lsf(a)
                if res is None:
                    raise ValueError(
                        f"compute asset '{a.name}' dispatch=lsf but no lsf resource "
                        "(set spec.lsf.default or asset.lsf with queue/cores/mem_mb/walltime)"
                    )
        return self
