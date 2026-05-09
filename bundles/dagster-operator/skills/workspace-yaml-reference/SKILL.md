---
name: workspace-yaml-reference
description: workspace.yaml — telling the webserver/daemon where your code lives. Covers python_module, gRPC code servers, and the cross-location AssetSpec pitfall.
---

<!-- all-might generated -->

# workspace-yaml-reference — `workspace.yaml`

## When to use

- User asks "how does dagster know about my code?"
- User asks "what's a code location?"
- User asks how to run multiple code locations
- User hits "Error loading base asset job" or other code-load errors

## What it is

`workspace.yaml` is **not** an instance-level config. It's the
file you pass to `dagster-webserver`, `dagster dev`, and CLI
commands to tell them which code locations to load.

```bash
dagster dev          -w workspace.yaml
dagster-webserver    -w workspace.yaml
dagster definitions list -w workspace.yaml
```

You typically keep it in your project root, alongside your
`Definitions` modules.

A "code location" is one Python process loading one
`Definitions(...)` object. You can have many code locations in
one `workspace.yaml`.

## Three loading modes

### Mode 1 — `python_module` (in-process)

The webserver / CLI imports the module directly into its own
process.

```yaml
# workspace.yaml
load_from:
  - python_module:
      module_name: my_pipelines
      location_name: pipelines
```

Use when:
- Single host, single project
- Development / `dagster dev`
- Quick CLI inspection (`dagster definitions list`)

**Don't use in production**: every webserver reload re-imports
your code into the webserver process. A bad import in user code
crashes the webserver.

### Mode 2 — `python_file`

```yaml
load_from:
  - python_file:
      relative_path: my_pipelines/definitions.py
      location_name: pipelines
```

Same in-process semantics as `python_module`, but lets you point
at a single file without packaging.

### Mode 3 — `grpc_server` (separate process — RECOMMENDED for prod)

The user runs a separate `dagster code-server start` process.
The webserver talks to it over gRPC.

```yaml
# workspace.yaml
load_from:
  - grpc_server:
      host: code-pipelines.internal
      port: 4000
      location_name: pipelines
```

Start the code server (typically as a systemd service):

```bash
dagster code-server start \
    -m my_pipelines \
    --host 0.0.0.0 \
    --port 4000 \
    --location-name pipelines
```

(Use `-m my_pipelines` to point at a Python module, or
`-f path/to/file.py` for a file.)

Use when:
- Production
- Multiple code locations with different deps / versions
- You want code reloads not to crash the webserver

To deploy code: restart only the code server (`systemctl restart
dagster-code-pipelines`). Webserver and daemon keep running.

## Multiple code locations

```yaml
load_from:
  - grpc_server:
      host: code-pipelines.internal
      port: 4000
      location_name: pipelines

  - grpc_server:
      host: code-foundry.internal
      port: 4001
      location_name: foundry

  - grpc_server:
      host: code-signoff.internal
      port: 4002
      location_name: signoff
```

Each runs an independent venv, can pin different Dagster
versions (within reason), and isolates code-load failures. UI
groups assets by `location_name`.

## Cross-location asset dependencies

This is the **hidden trap** that breaks "Error loading base
asset job" diagnoses (real case: LENS PoC Day 7).

### What works

In code location B, define an asset that depends on a key from
code location A by referencing the **AssetKey only** — don't
re-declare the upstream asset.

```python
# code location B (lib_upper) — assets.py
from dagster import asset, AssetKey, MaterializeResult, DataVersion, Definitions
import hashlib

@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],   # cross-loc dep
)
def signoff_report() -> MaterializeResult:
    payload = b"signoff_v1"
    digest = hashlib.sha256(payload).hexdigest()[:16]
    return MaterializeResult(
        data_version=DataVersion(digest),
        metadata={"signoff_digest": digest},
    )

defs = Definitions(assets=[signoff_report])
```

### What DOESN'T work — the trap

Don't `AssetSpec` the upstream key into the downstream
`Definitions(assets=...)`:

```python
# BROKEN — Dagster 1.13.3 errors with "Error loading base asset job"
from dagster import AssetSpec

external_lower_kit_summary = AssetSpec(
    key=AssetKey(["lib_lower", "kit_summary"]),
)

defs = Definitions(
    assets=[external_lower_kit_summary, signoff_report],   # BAD
)
```

**Why it breaks**: when the same `AssetKey` exists as a real asset
in location A and as an `AssetSpec` in location B's
`Definitions(assets=...)`, the implicit asset job builder in
1.13.3 sees two definitions for the same key and refuses to build
the job. The error surfaces at webserver load time as "Error
loading base asset job".

**Fix**: drop `external_lower_kit_summary` from
`Definitions(assets=...)`. Use `deps=[AssetKey(...)]` only. The
downstream asset will materialize fine — Dagster resolves the
cross-location dep through the workspace.

### Verifying cross-location wiring

```bash
# Both locations should appear
dagster definitions list -w workspace.yaml

# Each loads cleanly
dagster definitions validate -w workspace.yaml

# Asset graph spans both locations
dagster asset list -w workspace.yaml
```

If `validate` fails for one location with "base asset job", revisit
the AssetSpec issue above.

## Reload behavior

| Mode | Reload trigger | Blast radius |
|---|---|---|
| `python_module` / `python_file` | Webserver restart, or "Reload" button in UI | Re-imports into webserver — bad import crashes UI |
| `grpc_server` | Restart the code server process | Webserver fine, just that location goes UNLOADED briefly |

Production: always `grpc_server`. The reload-isolation alone
justifies the extra moving part.

## When to split into multiple locations

Split when:
- Different teams own different code
- One library needs a different Dagster / Python version
- One library has heavy native deps you don't want in the webserver
- You want failure isolation: bad code in one library doesn't
  break the others

Don't split for organization alone — assets within one location
can already use `key_prefix` and groups for grouping.

## Common pitfalls

### "Could not load location X: Connection refused"

Code server isn't running, or `host`/`port` in `workspace.yaml`
doesn't match where it's listening. `nc -zv code-pipelines.internal
4000`. Restart the code server.

### "ModuleNotFoundError: my_pipelines"

`python_module` mode but the webserver's venv doesn't have your
package installed. Either `pip install -e .` your project into
the webserver's venv, or switch to gRPC mode (where each code
server has its own venv).

### "Error loading base asset job"

See the cross-location AssetSpec trap above.

### Webserver shows old code after deploy

Forgot to reload. Click "Reload" in UI, or for `grpc_server`
restart the code server. The webserver does NOT poll for code
changes.

## Related

- Starting code servers: `skills/start-services/SKILL.md`
- Diagnosing code-load failures:
  `skills/diagnose-codeloc-fail/SKILL.md`
- Deploy verification: `skills/verify-deploy/SKILL.md`
