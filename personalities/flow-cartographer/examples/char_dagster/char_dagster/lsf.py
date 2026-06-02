"""Compatibility shim — see ``char_dagster/launchers.py`` for the
primary module. ``LSFLauncher`` / ``LSFJobSpec`` are re-exported here
so existing imports keep working; new code should import from
``char_dagster.launchers``, which also exposes ``MultiThreadLauncher``
behind the same interface.
"""
from char_dagster.launchers import JobSpec as LSFJobSpec  # noqa: F401
from char_dagster.launchers import LSFLauncher            # noqa: F401
