# Dagster-expert

Curated **All-Might personality bundles** for working with
[Dagster](https://dagster.io) in air-gap environments. Pinned to
**Dagster 1.13.3**.

Three personalities, each addressing a distinct slice of the
"work with Dagster offline" problem:

| Personality | Role | When you'd switch to it |
|---|---|---|
| [`dagster-operator`](bundles/dagster-operator/) | Air-gap ops agent | Bootstrapping, configuring, starting, verifying, diagnosing a self-hosted deployment |
| [`dagster-tutor`](bundles/dagster-tutor/) | Progressive teacher | Learning Dagster from `@asset` up through a production-shaped AP characterization flow (10 lessons, runnable code; lesson 09 mixes Perl + Python + TCL via Pipes; lesson 10 models corner/lvf/em branched characterization with cross-branch deps) |
| [`dagster-librarian`](bundles/dagster-librarian/) | Offline API reference | Looking up the right public API without internet (cheatsheet + runnable examples + SMAK-indexable corpus) |

The librarian is special: the operator and tutor **consult it
before generating Dagster API code from training memory** —
killing the "confidently wrong from outdated training" failure
mode that bites every LLM on a fast-evolving library.

## Why this exists

LLM agents in air-gap deployments (no `docs.dagster.io`, no
Stack Overflow, no Google) default to **generating from training
memory**. For Dagster — which had multiple API renames between
1.0 and 1.13 (`logical_version` → `data_version`,
`MaterializeResult` arg expansion, etc.) — this produces
confident-but-wrong code. Three observed failure modes from a
single Lesson 02 walkthrough:

- Used `dagster._core.definitions.data_version.extract_data_version_from_entry` (private path)
- Read tag `dagster/logical_version` (renamed → `dagster/data_version` in 1.13)
- Wrote `MultiPartitionsDefinition({a, b, c})` (1.13.3 limit: 2 dimensions)

The librarian's cheatsheet documents each of these as a known
gotcha; the lookup-api skill enforces "consult cheatsheet first,
generate from memory never".

## Audience

- **Humans** new to Dagster, especially in industrial / EDA flows
  (CAD characterization, simulation pipelines)
- **Less-capable internal LLM agents** (Kimi K2.5, MiniMax M2.5,
  offline Claude / GPT) running in corporate air-gap setups —
  these need explicit examples + cheatsheets, not "model thinks
  hard"

## Importing into your All-Might project

Each `bundles/<name>/` directory is a self-contained
[All-Might bundle](https://github.com/tl32rodan/All-Might).

```bash
# In your All-Might project root:
allmight import bundles/dagster-operator
allmight import bundles/dagster-tutor
allmight import bundles/dagster-librarian
```

(Or use `allmight share pull <git-url>` if you've published the
bundle through a git transport.)

After import, the librarian's vector indices need rebuilding:

```bash
# Build SMAK indices from the cheatsheet + examples corpus
mcp__smak__ingest \
  --config personalities/dagster-librarian/database/dagster-1.13.3/workspace_config.yaml \
  --index cheatsheet
mcp__smak__ingest \
  --config personalities/dagster-librarian/database/dagster-1.13.3/workspace_config.yaml \
  --index examples
```

Two indices, ~28 vectors total. Takes a few seconds.

## Architecture

```
                 ┌─────────────────────┐
                 │  dagster-librarian  │
                 │  (cheatsheet +      │
                 │   examples + SMAK)  │
                 └──────────┬──────────┘
                  consults  │
              ┌─────────────┴─────────────┐
              │                           │
      ┌───────▼────────┐         ┌────────▼───────┐
      │ dagster-operator│        │  dagster-tutor │
      │ (run + diagnose)│        │  (10 lessons)  │
      └────────────────┘         └────────────────┘
```

Both consumers carry a hard rule in their `ROLE.md`:

> **Never generate Dagster API code from training memory.** Before
> writing or recommending an API, EITHER `Read personalities/dagster-librarian/database/dagster-1.13.3/docs/<topic>.md`
> if known, OR `mcp__smak__search` against the librarian's corpus.

## Air-gap deployment

The bundles assume no internet at runtime. The
`dagster-operator/skills/bootstrap-airgap/` skill walks through
the wheelhouse pattern (`pip download` on connected host →
transfer → `pip install --no-index` on air-gap host).

For the librarian's corpus, the cheatsheet + examples are
markdown + Python files — ship them with the bundle, ingest into
SMAK on first import. No internet ever touched.

## Versioning

- Pinned to **Dagster 1.13.3** — every example is smoke-tested
  against this version
- All-Might bundle schema **v3**
- When Dagster ships a new minor (1.14.x), the librarian's
  curator creates `database/dagster-1.14.x/` alongside 1.13.3
  rather than mutating in-place; consumers can pin per-project

## Contributing

Two paths:

1. **For consumers using these bundles**: when you hit a Dagster
   gotcha not covered in the cheatsheet, file a case study to
   `personalities/dagster-librarian/memory/lessons_learned/_inbox/`
   in your All-Might project. The librarian's curator audits
   periodically and promotes to `database/dagster-1.13.3/docs/`.
2. **For pull requests against this repo**: open a PR with the
   updated bundle (regenerate via
   [`/one-for-all`](https://github.com/tl32rodan/All-Might) on
   your local All-Might project). Keep the manifest.yaml's
   `bundle_version` semver-bumped per change.

## License

Bundles' content is permissively licensed (see [LICENSE](LICENSE)).
Each bundled example is a minimal Dagster pattern — no
proprietary or vendor-internal content.
