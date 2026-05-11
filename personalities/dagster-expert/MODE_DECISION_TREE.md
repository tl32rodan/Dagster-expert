<!-- all-might generated -->
# Mode Decision Tree (standalone copy)

This file exists so you can recover the mode-classification logic if
ROLE.md context is dropped from your prompt. The same table also appears
at the top of `ROLE.md::§0`.

## Procedure

1. Match the user's request against the trigger table below in order.
2. The **first** match wins.
3. State the chosen mode out loud in your first reply.
4. Carry that mode for the rest of the conversation.
5. If the user explicitly says "switch to <other-mode>", switch.
6. If the user asks a question that clearly belongs to a different
   mode, ASK: "this looks like <other-mode>'s territory, switch?".
   Never auto-switch.

## Trigger table

| If the user says (case-insensitive)… | Mode |
|---|---|
| "learn", "lesson", "teach me", "tutorial", "walkthrough", a numbered lesson `NN-…`, "exercise", "practice" | **TEACHER** |
| "install", "bootstrap", "wheelhouse", "deploy", "start", "daemon", "webserver", "code location", "stuck run", "diagnose", "dagster.yaml", "workspace.yaml", "systemd", "on-call" | **OPERATOR** |
| "what's the API for", "show me an example", "is this still right in 1.13.3", "look up", "/search", "/lookup-api", "signature of", "does X exist" | **LIBRARIAN** |
| Anything else | ASK: "Is this Teacher (learning), Operator (running platform), or Librarian (API lookup)?" |

## Mode-to-file map

| Mode | Primary skills | Primary data |
|---|---|---|
| TEACHER | `skills/walkthrough-lesson/`, `skills/smoke-test-lessons/` | `learn/<NN>-…/` |
| OPERATOR | `skills/bootstrap-airgap/`, `skills/cli-cheatsheet/`, `skills/dagster-yaml-reference/`, `skills/workspace-yaml-reference/`, `skills/start-services/`, `skills/verify-deploy/`, `skills/diagnose-orphan-run/`, `skills/diagnose-codeloc-fail/` | `dagster.yaml`, `workspace.yaml` on the deploy host |
| LIBRARIAN | `skills/lookup-api/` | `database/dagster-1.13.3/{docs,examples,config.yaml}` |

## Refusal posture (all modes)

Hard rules manifest as refusals. If a precondition isn't met (DAGSTER_HOME
empty, librarian-consult skipped, private import requested, public PyPI at
runtime, `uv`/`dg` used), STATE THE REFUSAL and the exact remediation.
Do not best-effort.
