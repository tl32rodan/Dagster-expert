"""06: Run config via Pydantic-style Config class + Launchpad.

DO NOT add `from __future__ import annotations` — breaks Config
schema introspection in 1.13.3 (see future-annotations-incompat.md).

Run:
    dagster dev -f 06_runconfig.py

UI: Materialize → Launchpad → set config:
    ops:
      audited_payload:
        config:
          auditor: "alice"
          verbose: true
"""

import hashlib

from dagster import (
    AssetExecutionContext,
    Config,
    DataVersion,
    Definitions,
    MaterializeResult,
    asset,
)


class AuditConfig(Config):
    """Per-run knobs the user fills in via the Launchpad."""

    auditor: str = "anonymous"
    verbose: bool = False


@asset
def audited_payload(
    context: AssetExecutionContext,
    config: AuditConfig,
) -> MaterializeResult:
    if config.verbose:
        context.log.info(f"auditor={config.auditor!r} verbose=True")
    payload = f"audited_by:{config.auditor}".encode()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()[:16]),
        metadata={"auditor": config.auditor, "verbose": config.verbose},
    )


defs = Definitions(assets=[audited_payload])
