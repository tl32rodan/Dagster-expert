"""Package entry. Lazily exposes ``defs`` so that ``cardinality_calc.py``
(which imports ``pipelines.branches`` / ``pipelines.steps`` / ``pipelines.libraries``)
can run without Dagster installed.

``dagster dev -m pipelines`` still finds ``defs`` via PEP 562 ``__getattr__``.
"""


__all__ = ["defs"]


def __getattr__(name):
    if name == "defs":
        from .definitions import defs as _defs
        return _defs
    raise AttributeError(f"module 'pipelines' has no attribute {name!r}")
