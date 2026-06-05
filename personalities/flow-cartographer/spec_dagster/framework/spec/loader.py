"""M1 — spec loader. Scans flows/*/spec.yaml, validates, imports script
callables (fail at load, not runtime)."""
from __future__ import annotations

import importlib
from pathlib import Path

import yaml

from .schema import AssetSpec, FlowSpec


def import_callable(ref: str):
    """Import "module.path:callable" -> the callable. Raises at load time."""
    if ":" not in ref:
        raise ValueError(f"script ref must be 'module:callable', got {ref!r}")
    mod_name, attr = ref.split(":", 1)
    mod = importlib.import_module(mod_name)
    if not hasattr(mod, attr):
        raise ValueError(f"module '{mod_name}' has no callable '{attr}'")
    return getattr(mod, attr)


def load_spec(spec_path: str | Path) -> FlowSpec:
    data = yaml.safe_load(Path(spec_path).read_text())
    spec = FlowSpec.model_validate(data)
    # eagerly import every script callable so a bad ref fails at load
    for a in spec.assets:
        if a.script:
            import_callable(a.script)
    return spec


def load_all_specs(flows_dir: str | Path) -> list[FlowSpec]:
    """Load every flows/*/spec.yaml under flows_dir.

    Directories whose name starts with `_` are skipped — this is the
    convention for templates (`_template`) and temporarily-disabled
    flows (`_disabled_*`). Useful so a scaffold spec.yaml referencing
    a not-yet-renamed module path doesn't blow up the framework load.
    """
    root = Path(flows_dir)
    specs = []
    for spec_path in sorted(root.glob("*/spec.yaml")):
        if spec_path.parent.name.startswith("_"):
            continue
        specs.append(load_spec(spec_path))
    if not specs:
        raise ValueError(f"no */spec.yaml found under {root}")
    return specs
