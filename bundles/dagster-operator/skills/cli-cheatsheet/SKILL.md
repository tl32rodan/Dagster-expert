---
name: cli-cheatsheet
description: One-page Dagster CLI reference. The five binaries, the most-used subcommands, and the dg→dagster translation table.
---

<!-- all-might generated -->

# cli-cheatsheet — Dagster CLI quick reference

## When to use

- User asks "what's the CLI for X?"
- User shows a `dg ...` command and wants the equivalent
- You forgot the right flag

## The five binaries

| Binary | Purpose |
|---|---|
| `dagster` | Top-level CLI for instance / asset / job / run / debug ops |
| `dagster-daemon` | Runs schedules, sensors, run queue, run monitor |
| `dagster-webserver` | UI + GraphQL server |
| `dagster-webserver-debug` | UI loaded from a `.gz` debug snapshot (offline) |
| `dagster-graphql` | Headless GraphQL client |

All five come from `pip install dagster dagster-webserver
dagster-graphql`. They live in the venv's `bin/`.

## `dagster` subcommands you'll actually use

```bash
# Instance
dagster instance info
dagster instance migrate              # after install / upgrade

# Code location inspection
dagster definitions list     -w workspace.yaml
dagster definitions validate -w workspace.yaml

# Assets
dagster asset list           -w workspace.yaml
dagster asset list           -w workspace.yaml --select "key:my_asset+"
dagster asset materialize    -w workspace.yaml --select <key>
dagster asset materialize    -w workspace.yaml --select <key> --partition <p>
dagster asset wipe           -w workspace.yaml <key>     # CAREFUL

# Jobs
dagster job list             -w workspace.yaml
dagster job print            -w workspace.yaml -j my_job
dagster job execute          -w workspace.yaml -j my_job
dagster job execute          -w workspace.yaml -j my_job --config run_config.yaml
dagster job backfill         -w workspace.yaml -j my_job --partition-set my_set

# Runs
dagster run list                                # last 10 runs
dagster run list --limit 50
dagster run delete <RUN_ID>                     # destructive — keeps no event log

# Debug snapshots (postmortem)
dagster debug export <RUN_ID> /tmp/run.gz
dagster-webserver-debug /tmp/run.gz             # opens UI on the snapshot

# Schedules / sensors
dagster schedule list   -w workspace.yaml
dagster schedule status -w workspace.yaml <name>
dagster schedule start  -w workspace.yaml <name>
dagster schedule stop   -w workspace.yaml <name>
dagster sensor   list   -w workspace.yaml
dagster sensor   start  -w workspace.yaml <name>
dagster sensor   cursor -w workspace.yaml <name>

# All-in-one dev
dagster dev -w workspace.yaml -p 3000

# Code server (gRPC, prod)
dagster code-server start -m my_pipelines \
    --host 0.0.0.0 --port 4000 --location-name pipelines
```

## `dagster-daemon`

```bash
dagster-daemon run                  # foreground; usually under systemd
dagster-daemon liveness-check       # exit 0 if healthy — use as probe
```

Exactly **one** `dagster-daemon run` per Dagster instance.

## `dagster-webserver`

```bash
dagster-webserver -w workspace.yaml -h 0.0.0.0 -p 3000

# Read-only mode (UI works, no mutations / launches)
dagster-webserver -w workspace.yaml --read-only -p 3001
```

`--read-only` is good for analyst-facing instances: they see
runs and assets but can't launch or wipe.

## `dagster-graphql`

```bash
# Local introspection
dagster-graphql -w workspace.yaml -t '{ version }'

# Against a running webserver
dagster-graphql --remote http://webserver:3000/graphql \
    -t 'query { version }'

# With variables
dagster-graphql --remote http://webserver:3000/graphql \
    -t '
mutation Launch($key: String!) {
  launchPipelineExecution(executionParams: {
    selector: {pipelineName: "__ASSET_JOB"},
    runConfigData: {}
  }) { __typename }
}
' -v '{"key": "alpha"}'
```

## Asset selection syntax

Used with `--select`:

| Selector | Meaning |
|---|---|
| `alpha` | The asset `alpha` only |
| `alpha+` | `alpha` and everything downstream of it |
| `+alpha` | `alpha` and everything upstream |
| `++alpha` | `alpha` and 2 hops upstream |
| `*alpha*` | Glob match |
| `tag:foo=bar` | Assets tagged with `foo=bar` |
| `key:my/group/*` | Asset key prefix match |
| `group:my_group` | All assets in `my_group` |
| `code_location:pipelines` | All assets in that location |
| `a or b` / `a and b` | Combinations |

## `dg` and `uv` translation

If you read official Dagster docs that use `dg` or `uv`,
translate before pasting:

| Doc says | This deploy uses |
|---|---|
| `dg dev` | `dagster dev -w workspace.yaml` |
| `dg list defs` | `dagster definitions list -w workspace.yaml` |
| `dg launch -j J` | `dagster job execute -w workspace.yaml -j J` |
| `dg materialize -s K` | `dagster asset materialize -w workspace.yaml --select K` |
| `dg components ...` | **Don't use.** Components system is `dg`-only. Write `@asset`/`@op` directly. |
| `uv add dagster` | `pip install --no-index --find-links=~/wheelhouse dagster` |
| `uv run X` | `python -m X` (in activated venv) |
| `uvx ...` | not available; install package with pip into a venv |

## Most useful one-liners

```bash
# Health probe for monitoring
dagster-daemon liveness-check && \
    curl -fsS http://localhost:3000/server_info > /dev/null && echo OK

# Snapshot a problematic run for offline analysis
dagster debug export <RUN_ID> /tmp/run.gz

# What versions are installed?
pip list | grep -i dagster

# Where's DAGSTER_HOME?
echo $DAGSTER_HOME && ls -la $DAGSTER_HOME/dagster.yaml

# Grep daemon logs for a run id
journalctl -u dagster-daemon --since "1 day ago" | grep <RUN_ID_PREFIX>

# All five CLIs resolve to the venv?
which dagster dagster-daemon dagster-webserver dagster-webserver-debug dagster-graphql
```

## Destructive commands (use only with confirmation)

```bash
dagster run delete <RUN_ID>         # removes record + event log
dagster run wipe                    # WIPES ALL RUNS — refuse without explicit consent
dagster asset wipe <KEY>            # removes asset materialization records
dagster instance migrate            # schema change; safe to re-run but irreversible mid-flight
```

The personality's `permission` config blocks `dagster run wipe`
outright; the others ask before running.

## Related

- Daily ops chains: `skills/start-services/SKILL.md`,
  `skills/verify-deploy/SKILL.md`
- Configs the CLI reads: `skills/dagster-yaml-reference/SKILL.md`,
  `skills/workspace-yaml-reference/SKILL.md`
