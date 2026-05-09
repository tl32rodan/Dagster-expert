"""lab4: an asset with run config.

Run with: dagster dev -m configured
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
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={
            "auditor": config.auditor,
            "verbose": config.verbose,
        },
    )


defs = Definitions(assets=[audited_payload])
