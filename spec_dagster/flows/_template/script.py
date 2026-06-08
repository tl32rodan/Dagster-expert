"""script.py — pure functions that the framework's M2 builder calls.

THE RULES (whitepaper §0 + §4.3):
  1. DO NOT `import dagster`. This file must be a pure-Python module
     so the framework can run unit tests against it without spinning up
     Dagster, and so the same script is reused by both local-sim and
     LSF-launched workers.
  2. DO NOT use `@asset`, `@sensor`, or any Dagster decorators here.
     The framework wraps your functions.
  3. DO NOT bsub from your compute argv (use `dispatch: lsf` in spec
     instead — the launcher handles bsub at the run level so you don't
     create a nested bsub).
  4. Generator functions: signature is `gen_fn(*partition_values)` and
     return `dict[abs_path: str, content: str]`. The framework will
     write each file and compute the content_hash data_version over
     the concatenation.
  5. Compute functions: signature is `compute_fn(*partition_values)`
     and return `argv: list[str]`. The framework runs it via
     PipesSubprocessClient (or LSFRunLauncher in prod). The argv's
     final command should accept stdin/Pipes env vars; you can vendor
     a Pipes-aware inner script alongside this file (see liberate_char
     for an example).
  6. Verify: ``grep -E '^(from|import) dagster' script.py`` must be empty.

This template provides one generator and one compute, mirroring the
spec.yaml in this directory. Replace the bodies with your own logic.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _root() -> Path:
    """Output root, shared between generators (write) and compute
    (read). Configure via env so different runs / equivalence checks
    can point to different filesystems."""
    return Path(os.environ.get("MY_FLOW_ROOT", "/tmp/my_flow"))


def gen_thing(dim_a: str) -> dict[str, str]:
    """A trivial generator. Replace with actual content rendering.

    Returns one file per call: the framework writes it AND uses the
    concatenated content for a content_hash data_version. So change in
    `content` -> change in version -> downstream rebuild eagerly via
    standard Dagster staleness.
    """
    out = _root() / "intermediate" / f"thing_{dim_a}.txt"
    content = f"# generated for {dim_a}\nvalue={dim_a}\n"
    return {str(out): content}


def compute_command(dim_a: str) -> list[str]:
    """A trivial compute that just `echo`s — replace with your real
    tool invocation (e.g. EDA binary, simulator, model).

    The returned argv is run as a subprocess via PipesSubprocessClient
    on the same node where this run worker lives. In LOCAL-SIM mode
    the worker is the orchestrator host. In PROD (dispatch: lsf) the
    worker IS the LSF compute node, courtesy of LSFRunLauncher.

    NEVER prepend `bsub` here. The launcher (in prod) does that for you,
    once per run, at a higher layer. Prepending bsub here would be
    a nested bsub — see whitepaper §0 rule 4 + appendix A item 7.
    """
    return [
        sys.executable, "-c",
        f"print('compute fired for {dim_a}')",
    ]
