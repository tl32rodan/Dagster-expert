"""Package entry. Eagerly imports ``defs`` into the module namespace so
that Dagster's workspace loader (which iterates ``module.__dict__``,
not ``getattr``) finds the Definitions when ``workspace.yaml`` says
``module_name: pipelines``.

Do NOT switch to PEP 562 ``__getattr__`` lazy loading here — Dagster's
loader uses ``vars(module)`` / ``module.__dict__`` to scan for
Definitions / Job / RepositoryDefinition. Lazy attributes are invisible
to that scan and produce
``DagsterInvariantViolationError: No Definitions, RepositoryDefinition,
Job, Pipeline...`` at startup.
"""

from .definitions import defs  # noqa: F401

__all__ = ["defs"]
