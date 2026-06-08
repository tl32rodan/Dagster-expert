"""Data-version strategies (whitepaper appendix B).

resolve_version(name) -> a callable str-of-content -> version string.
  - "content_hash" : SHA256[:16] of the content (deterministic; same
                     content -> same version). This is what generator
                     assets use.
  - "timestamp"    : wall-clock (every call differs). FUTURE / not used
                     by liberate-char.
  - "module:fn"    : a custom callable imported from the flow.

Compute assets that report their own DataVersion via Pipes (e.g.
liberate's .ldb digest) do not use this — the framework leaves their
data_version to the reported MaterializeResult.
"""
from __future__ import annotations

import hashlib
import time
from typing import Callable


def content_hash_version(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def timestamp_version(_text: str = "") -> str:
    return str(time.time_ns())


def resolve_version(name: str) -> Callable[[str], str]:
    if name == "content_hash":
        return content_hash_version
    if name == "timestamp":
        return timestamp_version
    if ":" in name:
        from framework.spec.loader import import_callable

        return import_callable(name)
    raise ValueError(f"unknown version strategy: {name!r}")
