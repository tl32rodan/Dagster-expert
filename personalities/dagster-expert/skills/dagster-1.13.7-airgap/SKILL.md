# SKILL: dagster-1.13.7-airgap

> When to invoke: ANY task that requires writing or interpreting
> `from dagster import …` code, OR configuring `dagster.yaml` /
> `workspace.yaml`, OR running `dagster*` CLI commands on the
> air-gapped TSMC deploy.

## 0. Pre-flight (REFUSE if any fail)

```tcsh
echo $DAGSTER_HOME         # must be non-empty
which dagster              # must be inside the venv
dagster --version          # must report 1.13.7
```

bash equivalent:
```bash
echo $DAGSTER_HOME
which dagster
dagster --version
```

Refusals:
- `$DAGSTER_HOME` empty ⇒ tell user:
  `setenv DAGSTER_HOME /var/lib/dagster` (tcsh) /
  `export DAGSTER_HOME=/var/lib/dagster` (bash).
- `dagster --version` ≠ 1.13.7 ⇒ tell user to activate the 1.13.7 venv.
  Do NOT proceed against a different version; API drift is real.

## 1. Mandatory consult sequence

Before writing or interpreting any `from dagster import …`:

1. `Read personalities/dagster-expert/database/dagster-1.13.7/docs/INDEX.md`.
2. Route via the INDEX's table → read the matching `docs/<topic>.md`.
3. Read the corresponding `examples/<NN>_<topic>.py`.
4. Validate the example imports against the installed runtime:
   ```tcsh
   cd personalities/dagster-expert/database/dagster-1.13.7
   PYTHONPATH=. dagster definitions validate -m examples.NN_topic
   ```
5. **If steps 1–4 yield zero matches**: REFUSE. Tell the user
   "no librarian entry for `<topic>` — file an inbox case study
   (`personalities/dagster-expert/memory/lessons_learned/_inbox/<ISO>-<user>.md`)
   before I write this code". Do NOT fall back to training memory.

## 2. Hard rules — refuse to violate

1. **No private imports.** Refuse code reaching
   `dagster._core.*` / `_internal.*` / `_private.*` / `_utils.*`.
2. **No `dg` / `uv` / `pipx` / Poetry / k8s / Helm / Dagster+ / Cloud /
   Components / public PyPI / Docker registries / telemetry.** See
   `docs/AIRGAP_DELTAS.md` for substitutes.
3. **`telemetry: { enabled: false }`** in every `dagster.yaml`.
4. **Absolute paths** in every `dagster` command:
   `dagster ... -w /abs/path/workspace.yaml -m fully.qualified.module`.
5. **No `cd` chains.** State-leak across turns.
6. **Air-gap pip**: `pip install --no-index --find-links=~/wheelhouse X`
   ONLY. Refuse `pip install X` (defaults to public PyPI).
7. **No destructive ops without explicit consent**: `dagster run wipe`,
   `dagster asset wipe`, dropping Postgres tables, `rm -rf
   $DAGSTER_HOME`.

## 3. Shell-aware output

User's session is **tcsh**. Show env-var commands as:

```tcsh
setenv DAGSTER_HOME /var/lib/dagster
setenv DAGSTER_PG_PASSWORD <secret>
```

with the bash equivalent in parentheses:
```bash
# (bash equivalent: export DAGSTER_HOME=/var/lib/dagster)
```

## 4. Visible state checkpoints

At the start of any multi-step task, print:
```tcsh
echo $DAGSTER_HOME
which dagster
dagster --version
ls -la $DAGSTER_HOME/dagster.yaml
```

Both user and agent see state; nobody guesses.

## 5. Production paths (this deploy)

Resolve once at session start by asking the user if any are unset:

- Wheelhouse: `[~/wheelhouse/]`
- Production `DAGSTER_HOME`: `[/var/lib/dagster]`
- Dev `DAGSTER_HOME`: `[~/.dagster]`
- Workspace: `[~/projects/<project>/workspace.yaml]`
- Postgres: `[pg.internal:5432]`
- Code servers: `[code-pipelines.internal:4000]`
- Webserver URL (if running): `[http://webserver.internal:3000]`

## 6. Common operations cheat sheet

```tcsh
# Validate definitions
dagster definitions validate -m flows.liberate_char.definitions

# Materialize one asset
dagster asset materialize -w /abs/workspace.yaml --select my_asset

# Materialize partition
dagster asset materialize -w /abs/workspace.yaml \
  --select my_asset --partition "tt_25|INV"

# Run a job
dagster job execute -w /abs/workspace.yaml -j my_job

# List runs
dagster run list -w /abs/workspace.yaml

# Service entry points
dagster-daemon run -w /abs/workspace.yaml
dagster-webserver -w /abs/workspace.yaml  # optional; production skips
```

## 7. When the question is about the Execution Fabric framework

If the user asks "how does our framework do X" or names a path under
`execution_fabric/`, you are out of scope. Direct the user to
**`/WHITEPAPER.md`** (repo root) — that's where the framework's
architecture lives. This skill is about Dagster the library, not about
how we use it.
