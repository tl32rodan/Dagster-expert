<!-- all-might generated -->
## Routing — pick the active personality

Before running anything below, identify which personality should act
and substitute its name for ``<active>`` in every path.

1. **Explicit mention** — if the user named a personality (e.g.
   "for stdcell_owner ..."), use it.
2. **Conversation context** — if recent turns are clearly about
   one personality's domain (workspace name, role keywords from
   that personality's ``ROLE.md``), use it.
3. **Default** — read the leading callout at the top of
   ``MEMORY.md``::

       > **Default personality**: <name>

   Use ``<name>``. If the callout is absent and only one personality
   is registered (one row in ``MEMORY.md``'s project map), that one
   is the implicit default.

If none of these resolves, ask the user before proceeding — never
guess. The same routing applies to every step below.

Search the codebase by semantic meaning.

SMAK searches the vector index — source files are never copied.
Results point back to files at their original paths.

## How to execute

```bash
smak search "<query>" --config personalities/<active>/database/<workspace>/config.yaml --index source_code --top-k 5 --json
```

To search across all corpora at once:
```bash
smak search-all "<query>" --config personalities/<active>/database/<workspace>/config.yaml --top-k 3 --json
```

To look up a specific symbol by UID:
```bash
smak lookup "<file_path>::<symbol_name>" --config personalities/<active>/database/<workspace>/config.yaml --index source_code --json
```

## What to expect

JSON output with a `results` array. Each result contains:
- `id` — the matched chunk/symbol identifier
- `text` or `content` — the matched source code
- `score` — relevance score (0–1)
- `metadata` — file path, symbol name, etc.

## After searching

- If a result has a sidecar (`.{filename}.sidecar.yaml` beside it), read the
  sidecar to see its enriched intent and relations.
- If a result has NO sidecar or missing intent, consider enriching it with `/enrich`.
- Present results to the user in terms of "knowledge graph" — do not mention SMAK.
