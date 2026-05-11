---
name: diagnose-codeloc-fail
description: Code location won't load. Walk through "Could not load location", "ModuleNotFoundError", and the cross-location "Error loading base asset job" Day7 case.
---

<!-- all-might generated -->

# diagnose-codeloc-fail — code location won't load

## When to use

- UI shows "Code location <X> failed to load" (red banner)
- `dagster definitions validate -w workspace.yaml` errors
- "Error loading base asset job" anywhere
- "ModuleNotFoundError" / "ImportError" on dagster startup

## First — get the actual error message

The UI banner usually truncates. Get the full traceback:

```bash
dagster definitions validate -w /etc/dagster/workspace.yaml
```

If that exits 0 but UI still red, the webserver and CLI are
loading different `workspace.yaml`s. Check `-w` paths match.

For gRPC code servers, the error is in the code server's logs,
not the webserver:

```bash
journalctl -u dagster-code-<location-name> -n 100 --no-pager
```

## Triage tree

### Symptom A — "Could not load code location: Connection refused"

The webserver can't reach the gRPC code server.

```bash
# Is it running?
systemctl status dagster-code-<name>

# Is the port open?
nc -zv <host> <port>          # use the host/port from workspace.yaml
```

Fix:
- Code server crashed → check its journal, restart
- Wrong host/port in `workspace.yaml` → fix and reload webserver
- Firewall blocks the port → IT issue

### Symptom B — "ModuleNotFoundError: <pkg>"

Code references a Python package that isn't installed in the
process loading the code.

For `python_module` mode: webserver's venv is missing the package.

```bash
# Which venv is the webserver using?
ps -ef | grep dagster-webserver
# The first arg is the venv's python path

source /opt/dagster-venv/bin/activate
pip install --no-index --find-links=~/wheelhouse <missing-pkg>
sudo systemctl restart dagster-webserver
```

For gRPC mode: code server's venv is missing the package.

```bash
sudo -u dagster -i
source /opt/dagster-code-pipelines-venv/bin/activate
pip install --no-index --find-links=~/wheelhouse <missing-pkg>
exit
sudo systemctl restart dagster-code-pipelines
```

If the package isn't in the wheelhouse: see
`skills/bootstrap-airgap/SKILL.md` for adding wheels.

### Symptom C — "ImportError: cannot import name X from Y"

Version skew. Two packages built against different dagster
versions, or the code references API that moved between minor
versions.

```bash
# What's actually installed in the loading venv?
pip list | grep -i dagster
```

Fix: align all `dagster*` packages to the same version. If user
upgraded `dagster` but not `dagster-postgres`, that's the cause.

```bash
pip install --no-index --find-links=~/wheelhouse \
    "dagster==1.13.3" \
    "dagster-postgres==1.13.3" \
    "dagster-webserver==1.13.3" \
    "dagster-graphql==1.13.3"
```

### Symptom D — "Error loading base asset job"

This is the Day7 LENS PoC bug. Typical cause: cross-location
asset graph has the same `AssetKey` defined as both a real asset
in code location A AND as an `AssetSpec` in code location B's
`Definitions(assets=...)`.

**Fix**: in code location B, drop the `AssetSpec` from
`Definitions(assets=...)`. Use only `deps=[AssetKey([...])]` on
the downstream asset.

```python
# WRONG — Day7 broken pattern
from dagster import AssetSpec, AssetKey, Definitions, asset

external_lower_kit = AssetSpec(key=AssetKey(["lib_lower", "kit_summary"]))

@asset(deps=[AssetKey(["lib_lower", "kit_summary"])])
def signoff_report(): ...

defs = Definitions(assets=[external_lower_kit, signoff_report])  # ← drop external_lower_kit
```

```python
# RIGHT — only the downstream is in this Definitions
from dagster import AssetKey, Definitions, asset

@asset(
    key_prefix=["lib_upper"],
    deps=[AssetKey(["lib_lower", "kit_summary"])],
)
def signoff_report(): ...

defs = Definitions(assets=[signoff_report])
```

After fix, restart the code server for location B and re-validate.

### Symptom E — "PermissionError" reading code or DAGSTER_HOME

Service user (e.g. `dagster`) doesn't own the code or
`DAGSTER_HOME` paths. Check ownership:

```bash
ls -la /opt/my_pipelines
ls -la /var/lib/dagster
```

Fix:
```bash
sudo chown -R dagster:dagster /opt/my_pipelines /var/lib/dagster
```

### Symptom F — code server starts then dies immediately

```bash
journalctl -u dagster-code-<name> -n 200
```

Look at the last few lines before exit. Most common:
- Syntax error in user code → user fixes the code
- Package import does heavy work at import time → user moves
  it inside the function
- `Definitions(...)` raises at construction → user code throws
  during definition load

### Symptom G — `python_module` works locally, gRPC fails

Most often: `cwd` differs. gRPC code server starts in `WorkingDirectory=`
of the systemd unit; if the user code does `open("config.yaml")`
with a relative path, it'll FileNotFoundError.

Fix: use absolute paths in code, or set `WorkingDirectory=` to
match where the user expects to be.

## Workflow when you don't recognize the error

1. `dagster definitions validate -w workspace.yaml` — read the
   full traceback.
2. The traceback's last few frames point at user code, not
   Dagster internals — focus there.
3. If it's a Dagster internal frame, the error usually has
   "AssetKey" or "Definitions" or "asset job" in it — check
   Symptom D.
4. Reproduce in a Python REPL:

   ```python
   import importlib
   m = importlib.import_module("my_pipelines.definitions")
   print(m.defs)
   ```

   If this crashes the same way, you isolated the failure from
   Dagster's loader. Easier to debug.

5. If it only fails under the gRPC code server but not REPL,
   suspect cwd / env vars — see Symptom G.

## After fixing — what to restart

| What you changed | Restart |
|---|---|
| User code in a code location | That code server only |
| `workspace.yaml` | Webserver + daemon |
| `dagster.yaml` | Webserver + daemon (+ code servers if env-dependent) |
| Installed/upgraded a wheel | All processes that import it |

After restart, re-run validation:

```bash
dagster definitions validate -w workspace.yaml
```

## Common pitfalls

### Webserver "Reload" button doesn't pick up changes

For `python_module` mode, "Reload" re-imports — usually works.
For `grpc_server` mode, "Reload" tells the code server to
reload, BUT some import-time side effects don't get cleared.
When in doubt, restart the code server process.

### Code loads but jobs are missing

The asset/job decorator wasn't actually executed during
`Definitions(...)` construction. Check that the user is
importing the module that defines the asset, e.g.

```python
# my_pipelines/definitions.py
from my_pipelines.assets import alpha, beta   # must import
defs = Definitions(assets=[alpha, beta])
```

### "AssertionError: ..." with no clear message

Often a Dagster-internal sanity check on the user's
asset/dependency graph. Read the traceback closely; the message
above the traceback usually names the offending key.

## Related

- Cross-location semantics: `skills/workspace-yaml-reference/SKILL.md`
- Wheelhouse: `skills/bootstrap-airgap/SKILL.md`
- After fix, run health checks: `skills/verify-deploy/SKILL.md`
