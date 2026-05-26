# Dagster-expert

An air-gapped **Dagster 1.13.3** agent project — a set of
[All-Might](https://github.com/tl32rodan/All-Might) **v4 personalities**
for working with [Dagster](https://dagster.io) on machines with no
internet (no `docs.dagster.io`, no public PyPI, no Dagster+ / Cloud).
Built for **less-capable agents** (MiniMax M2.5, Kimi K2.5, offline
Claude / GPT) driving TSMC air-gap workstations, so the instructions are
mechanical and checklist-driven rather than "model thinks hard".

Two personalities live side by side under `personalities/`:

| Personality | Capabilities | What it does |
|---|---|---|
| [`dagster-expert`](personalities/dagster-expert/) | database, memory | Daily driver. One personality, three internal modes — **TEACHER** (20 progressive lessons), **OPERATOR** (bootstrap / run / diagnose a self-hosted air-gap deployment), **LIBRARIAN** (offline public-API lookup). |
| [`flow-cartographer`](personalities/flow-cartographer/) | memory, schedule | Given any execution flow (`$FLOW_SRC` + a `CONVERSION.md` charter), runs a scheduled **plan → build → verify → reflect** loop that converts it to Dagster 1.13.3 one verified increment at a time, until the charter's success criteria are met. |

`dagster-expert` was assembled by merging three earlier v3 bundles
(`dagster-operator` + `dagster-tutor` + `dagster-librarian`) into a single
mode-switching personality. `flow-cartographer` evolved in place from the
retired `dagster-ap-auditor`; it **reads** `dagster-expert`'s API corpus,
lessons, and demo as ground truth and never duplicates them.

> Mode / personality switching is internal: tell the agent "switch to
> OPERATOR" or "switch to flow-cartographer". There is no CLI command —
> the agent routes by the Mode Decision Tree at the top of each `ROLE.md`.

## Why this exists

LLM agents in air-gap deployments default to **generating Dagster API
code from training memory**. For Dagster — which had multiple API renames
between 1.0 and 1.13 (`logical_version` → `data_version`,
`MaterializeResult` arg expansion, `MultiPartitionsDefinition`'s 2-axis
limit, etc.) — that produces confident-but-wrong code. Three real failure
modes from a single Lesson 02 walkthrough:

- Used `dagster._core.definitions.data_version.extract_data_version_from_entry` (private path)
- Read tag `dagster/logical_version` (renamed → `dagster/data_version` in 1.13)
- Wrote `MultiPartitionsDefinition({a, b, c})` (1.13.3 limit: 2 dimensions)

The fix is a hard rule both personalities carry:

> **Never generate Dagster API code from training memory.** Before writing
> or recommending an API, EITHER `Read
> personalities/dagster-expert/database/dagster-1.13.3/docs/<topic>.md`,
> OR search the LIBRARIAN corpus with the `lookup-api` skill. Zero
> results ⇒ refuse, don't guess.

The LIBRARIAN corpus
(`personalities/dagster-expert/database/dagster-1.13.3/`) holds the
cheatsheet `docs/` + runnable `examples/`, each documenting a known
gotcha; `flow-cartographer`'s `verify` tick enforces the same
public-API-only check on every increment it builds.

## Audience

- **Humans** new to Dagster, especially in industrial / EDA flows
  (CAD characterization, simulation pipelines).
- **Less-capable internal LLM agents** (Kimi K2.5, MiniMax M2.5, offline
  Claude / GPT) in corporate air-gap setups — these need explicit
  examples + cheatsheets + mechanical pre-flight checklists, not
  open-ended reasoning.

## Layout

```
personalities/
  dagster-expert/              # database + memory; TEACHER / OPERATOR / LIBRARIAN
    ROLE.md                    #   mode decision tree + per-mode workflow
    learn/                     #   20 progressive lessons (01 → 20)
    database/dagster-1.13.3/   #   offline API corpus: docs/ + examples/
    demo/scale-lib/            #   production-shaped 4-layer / folder-as-asset reference
    skills/                    #   bootstrap-airgap, cli-cheatsheet, lookup-api, …
  flow-cartographer/           # memory + schedule; the conversion loop
    ROLE.md                    #   §0 Wake SOP (first action every tick)
    CONVERSION.md              #   the user-owned charter ($FLOW_SRC + goals)
    flow-model/                #   live conversion state (ledger, steps, open questions)
    conversion-coverage/       #   the 5 behaviors a conversion must preserve
    skills/                    #   wake, plan-loop, build-loop, verify-loop, reflect-loop
    scheduled/                 #   the four am-flow-cartographer-<tick> tasks
```

See `AGENTS.md` for the full map and
`personalities/<name>/QUICKSTART.{en,zh}.md` for a per-personality intro
(bilingual EN / 繁中).

## Using these personalities

This repository **is** an All-Might v4 project. Two ways to use it:

- **Directly** — open it in an All-Might-aware harness; the `role-load`
  hook injects every `personalities/*/ROLE.md`, so both personalities are
  in context and you switch modes by asking.
- **Transfer into another All-Might project** — bundle a personality with
  the [`/one-for-all`](https://github.com/tl32rodan/All-Might) skill, then
  absorb it on the target with `/all-for-one`. (That is how
  `dagster-expert` itself was assembled from the three original bundles.)

The LIBRARIAN's SMAK vector indices are gitignored and rebuilt locally
from the corpus via the `database` capability's `/ingest` skill against
`personalities/dagster-expert/database/dagster-1.13.3/config.yaml`. No
internet is ever touched.

## Air-gap deployment

The personalities assume no internet at runtime. `dagster-expert`'s
`skills/bootstrap-airgap/` walks through the wheelhouse pattern
(`pip download` on a connected host → transfer → `pip install --no-index`
on the air-gap host). The API corpus ships as markdown + Python in the
repo and is ingested into SMAK on first use — nothing is fetched online.

## Versioning

- Pinned to **Dagster 1.13.3** — every lesson + example is smoke-tested
  against this version.
- All-Might **schema v4** personalities (`manifest.yaml::schema_version`).
- When Dagster ships a new minor (1.14.x), the LIBRARIAN curator creates
  `database/dagster-1.14.x/` alongside 1.13.3 rather than mutating
  in-place; consumers pin per project.

## Contributing

- **Hit a Dagster gotcha** not covered in the corpus? File a case study
  to `personalities/dagster-expert/memory/lessons_learned/_inbox/`; the
  curator promotes it into `database/dagster-1.13.3/docs/`.
- **PRs against this repo**: keep each `manifest.yaml`'s lineage + notes
  current, and bump the relevant version field per change.

## License

Content is permissively licensed (see [LICENSE](LICENSE)). Each bundled
example is a minimal Dagster pattern — no proprietary or vendor-internal
content.
