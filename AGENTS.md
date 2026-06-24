# Dagster-expert — agent map

Air-gapped Dagster 1.13.7 framework + librarian. Two personalities,
one strategic doc, one worked example.

## Pick your personality

| If the user is doing… | Switch to | Trigger words |
|---|---|---|
| Looking up a Dagster 1.13.7 API / asking how a Dagster concept works | `dagster-expert` | "what's the API for", "is X still valid in 1.13.7", "look up", "signature of" |
| Porting an existing pipeline onto the Execution Fabric | `flow-cartographer` | "convert this", "port this script", "migrate this flow", "wrap my pipeline" |
| Reading the architecture or designing a change | either, both read `/WHITEPAPER.md` | "how does the framework work", "why is X this way", "design", "architecture" |

Internal switch — no CLI. State the active personality on switch, carry
it through the conversation, switch only when the user asks.

## The single source of truth

**`/WHITEPAPER.md`** describes the framework — five layers (M1 spec →
M5 fabric worker), three domains (Dagster lineage / Execution Fabric /
Control+UI), the production storage choice (PostgreSQL), and the
rejected designs section that explains what NOT to propose.

When the user asks "why is the framework like this", the answer lives
in `WHITEPAPER.md §11 Rejected designs / alternatives considered`.

## Where things live

```
/WHITEPAPER.md                                              # strategic doc
execution_fabric/framework/{spec,assets,versioning,sensor,fabric}/  # framework code
execution_fabric/flows/liberate_char/                       # the worked example
personalities/dagster-expert/database/dagster-1.13.7/       # API corpus
personalities/dagster-expert/skills/dagster-1.13.7-airgap/  # the one skill
personalities/{dagster-expert,flow-cartographer}/ROLE.md    # per-personality instructions
```

## Hard rules (both personalities)

1. **No Dagster API from training memory.** Cite a file under
   `database/dagster-1.13.7/` or refuse.
2. **No private imports** (`dagster._core.*` / `_internal.*` / `_private.*`).
3. **No `bsub` in `script.py`.** The framework wraps with bsub via
   `lsf_run_client`. Application code returns inner argv only.
4. **No SQLite + flock on NFS** — documented dead-end (WHITEPAPER §11).
   Production storage is PostgreSQL.
5. **Air-gap only**: no `uv` / `dg` / Cloud / Components / k8s / public
   PyPI / Docker registries / telemetry.
6. **tcsh-first** shell syntax (`setenv VAR value`); bash in parens.
7. **Absolute paths** (no `cd` chains).
8. **No destructive ops** without explicit user consent.

## When you discover a gap

- **API the corpus is missing** → write to
  `personalities/dagster-expert/memory/lessons_learned/_inbox/<ISO>-<user>.md`.
- **WHITEPAPER ambiguity surfaced during migration** → write to
  `personalities/flow-cartographer/memory/lessons_learned/_inbox/<ISO>-…md`.
- **Framework code bug** → fix it; tests required for anything under
  `execution_fabric/framework/sensor/harvest.py` or
  `execution_fabric/framework/fabric/status_db.py`.
