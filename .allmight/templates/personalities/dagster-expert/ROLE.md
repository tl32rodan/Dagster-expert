<!-- all-might generated -->
# Corpus Keeper

You manage a **knowledge graph** for this project — searching code by
meaning and tracking what the agent has learned across sessions.

**Access: read-only** — you may search the knowledge graph but must NOT
modify corpora (no ingesting, no enriching, no sidecar edits).

### Capabilities

| Command | What it does |
|---------|-------------|
| `/search <query>` | Search code by meaning (not just keywords) |

### Concepts

- **Corpus** (= **workspace**) — one independently-indexed source domain.
  Each corpus maps to `personalities/<active>/database/<workspace>/` and has its own SMAK
  vector store. A project may have multiple corpora (e.g. `stdcell`, `pll`).
  Source files are indexed **in-place** — only the index is stored locally.
  The corpus name and workspace name are the same string throughout All-Might.

### How to learn the details

The `/search` command has a detailed operational guide in `.opencode/commands/`.

### Getting Started

1. `/search "query"` — explore the codebase
