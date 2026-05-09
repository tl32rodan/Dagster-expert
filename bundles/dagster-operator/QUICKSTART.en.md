# QUICKSTART — `dagster-operator` (English)

You're the operator of an air-gapped Dagster deployment. This page
points you at the right skill for what you need RIGHT NOW.

## "I'm starting from nothing"

Read in this order:

1. **`PREREQUISITES.md`** — confirm host, Python, Postgres,
   network are ready.
2. **`skills/bootstrap-airgap/SKILL.md`** — build wheelhouse on
   connected host, install on air-gap host.
3. **`skills/dagster-yaml-reference/SKILL.md`** — write
   `$DAGSTER_HOME/dagster.yaml` (start with the prod template).
4. **`skills/workspace-yaml-reference/SKILL.md`** — write
   `workspace.yaml` (start with `python_module` for dev, switch
   to `grpc_server` before going live).
5. **`skills/start-services/SKILL.md`** — start webserver, daemon,
   code servers (systemd units provided).
6. **`skills/verify-deploy/SKILL.md`** — run the 5-step health
   chain. Stop at the first failure.

If any step fails, the skill tells you which `diagnose-*` skill
to escalate to.

## "Something is broken"

| Symptom | Skill |
|---|---|
| Run stuck in STARTED forever | `skills/diagnose-orphan-run/` |
| "Code location failed to load" | `skills/diagnose-codeloc-fail/` |
| "Error loading base asset job" | `skills/diagnose-codeloc-fail/` (Symptom D — cross-loc AssetSpec trap) |
| "ModuleNotFoundError" | `skills/diagnose-codeloc-fail/` (Symptom B) |
| Webserver returns 502 / nothing | `skills/verify-deploy/` |
| Daemon liveness-check fails | `skills/verify-deploy/` Step 2 + journalctl |

## "I just need to look up a command"

`skills/cli-cheatsheet/` — the five binaries, the `dagster`
subcommands, the `dg`→`dagster` translation table.

## What this agent will NOT do

- Set up Dagster+ / Cloud / Insights / Hybrid
- Set up Kubernetes / Helm / `K8sRunLauncher`
- Suggest `uv`, `dg`, `pipx`, Poetry, public PyPI at runtime
- Run `dagster run wipe` without explicit consent
- `rm -rf` user data without confirmation
- Enable telemetry

If you ask for any of these, the agent will explain why it's out
of scope and propose the air-gap-friendly alternative.

## "I want to learn Dagster, not operate it"

Switch to the `dagster-tutor` personality. It walks through 8
progressive lessons (asset → deps → partitions → run config →
failures → cancel/restart → cross-location → complex DAGs).

Tell the agent: **"switch to dagster-tutor"**. The agent will
update `MEMORY.md`'s `> **Active personality**:` callout and load
the tutor's role. (There is no CLI subcommand; the active
personality lives as one line in `MEMORY.md`.)

## How to give this agent feedback

Found a wrong recipe? An air-gap gotcha not covered? File a
case study via `/remember` — it goes to
`memory/lessons_learned/_inbox/<ISO>-<user>.md`. Brian (the
curator) audits and promotes worthwhile ones to canonical
knowledge.

Do NOT edit `memory/understanding/canonical.md` or
`rules/` directly — they're curator-only.

## Quick reference card

```bash
# Activate
source /opt/dagster-venv/bin/activate
export DAGSTER_HOME=/var/lib/dagster

# Health
dagster instance info
dagster-daemon liveness-check
curl -fsS http://localhost:3000/server_info
dagster definitions validate -w /etc/dagster/workspace.yaml

# Daily ops
dagster run list --limit 20
dagster definitions list -w /etc/dagster/workspace.yaml
dagster asset materialize -w /etc/dagster/workspace.yaml --select <key>

# Postmortem
dagster debug export <RUN_ID> /tmp/run.gz
dagster-webserver-debug /tmp/run.gz
```

That's it. Pick a skill, read it, run the commands, verify.
