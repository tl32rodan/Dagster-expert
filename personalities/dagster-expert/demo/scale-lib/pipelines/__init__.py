"""scale-lib demo — Tier-1 Dagster wrapping a per-library characterization flow.

Public entry: ``defs`` (a Dagster ``Definitions``). Imported eagerly so
``dagster dev -m pipelines`` autodiscovery finds it.

Architecture (four layers; only Layer 3+ touches Dagster):

* ``spec/`` — pure data, zero Dagster import.
* ``rules/`` — concrete dep rules (each file = one rule).
* ``registry.py`` — single source of truth: ``DEPS = DepRegistry([...])``.
* ``translator.py`` — converts ``PartitionRule`` to Dagster ``PartitionMapping``.
* ``factory.py`` + ``definitions.py`` — builds ``@asset`` decorations.

The layer separation is enforced by ``tests/test_layer_imports.py``,
which greps source files for ``import dagster`` and verifies the
forbidden layers (``spec/``, ``rules/``, ``registry.py``) are clean —
this is independent of what *this* module imports.
"""
from .definitions import defs  # noqa: F401

__all__ = ["defs"]
