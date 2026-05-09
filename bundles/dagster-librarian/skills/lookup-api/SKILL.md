---
name: lookup-api
description: Find the right Dagster public API offline. Strict search order — cheatsheet → examples → pydoc → help() → dir() → source. Refuses to reach into dagster._core.* / _internal.*. For air-gap consumers (Kimi K2.5, MiniMax M2.5) and any agent that should NOT generate Dagster API from training memory.
---

<!-- all-might generated -->

# lookup-api — offline Dagster API discovery

## When to use

- An agent (yourself or another personality) is about to generate
  Dagster code and is **not 100% sure** the API is correct in
  the pinned Dagster version (1.13.3)
- A version-sensitive question: "what's the tag for X in 1.13.3?",
  "is `Y` still public?", "is `Z` deprecated?"
- A user asks "find me the right way to do X in Dagster"

## Hard rule

**Never generate from memory** for version-sensitive APIs. Always
go through the search order below. If no answer is found, **stop
and ask the user** — don't fabricate.

## Search order (top to bottom; stop at first answer)

### 1. Cheatsheet — `database/dagster-1.13.3/docs/`

```bash
# Topic known
Read personalities/dagster-librarian/database/dagster-1.13.3/docs/<topic>.md

# Topic unknown — start with INDEX
Read personalities/dagster-librarian/database/dagster-1.13.3/docs/INDEX.md
```

Topics covered (from INDEX.md):
- `asset-basics.md`
- `style-a-vs-b.md`
- `data-version-and-staleness.md`
- `partitions.md`
- `runconfig.md`
- `failures-retries.md`
- `cross-location.md`
- `tags-and-versions.md`
- `future-annotations-incompat.md`
- `api-discovery-offline.md`
- `avoid-private-imports.md`

If the cheatsheet answers the question, you're done.

### 2. Examples — `database/dagster-1.13.3/examples/`

```bash
ls personalities/dagster-librarian/database/dagster-1.13.3/examples/
Read personalities/dagster-librarian/database/dagster-1.13.3/examples/<NN>_*.py
```

Examples are smoke-tested against Dagster 1.13.3, so they're
guaranteed to import + load. Use as ground truth when the
cheatsheet entry is ambiguous.

### 3. SMAK semantic search

```python
mcp__smak__search query="data version propagation when upstream changes"
# returns ranked entries from the librarian's corpus + snippets
```

Use when the topic is fuzzy or you don't know which cheatsheet
file to open.

### 4. `pydoc` (Python's built-in offline doc tool)

```bash
python -m pydoc dagster.MaterializeResult
python -m pydoc dagster.AssetExecutionContext
python -m pydoc dagster.RetryPolicy
```

Returns full constructor signature + docstring with `Args:`
describing each parameter. Always works air-gap.

### 5. `help()` in REPL

```python
python -c "import dagster; help(dagster.DataVersion)"
```

Same content as pydoc, scriptable.

### 6. `dir()` filtered

```python
python -c "
import dagster
public = [a for a in dir(dagster) if not a.startswith('_')]
print([a for a in public if 'Version' in a])
"
# → ['DataVersion', 'DataVersionsByPartition']
```

Or the structured public-class filter:

```python
python -c "
import dagster
classes = [
    a for a in dir(dagster)
    if not a.startswith('_')
    and isinstance(getattr(dagster, a), type)
    and getattr(getattr(dagster, a), '_is_public', False)
]
print(classes)
"
```

### 7. Source (last resort)

```bash
DAGSTER_DIR=$(python -c "import dagster, os; print(os.path.dirname(dagster.__file__))")
ls $DAGSTER_DIR
```

Read code only to confirm what `pydoc` already told you. **Do
not import from any path that contains `_core`, `_internal`,
`_private`** — these are private and may break in any minor
version.

## What to do if NO public API exists

1. **Stop.** Don't smuggle in a `_core` import.
2. **Tell the user**: "No public API found for X. Options:
   (a) reformulate the design — usually a Style A vs Style B
   confusion (`docs/data-version-and-staleness.md`),
   (b) file a `/remember` entry to flag the gap,
   (c) accept the limitation and work around it."
3. If the limitation is real, **add a cheatsheet entry** so the
   next agent doesn't repeat the search.

## Worked example

**Question**: "How do I read upstream's data_version inside an
asset that uses explicit `deps=[...]`?"

| Step | Action | Result |
|---|---|---|
| 1 | Read `docs/data-version-and-staleness.md` | Found! Don't query metadata; redesign as Style A or filesystem-read |
| Done | — | Answer the user, link to the cheatsheet entry |

Without the cheatsheet (hypothetical):
| Step | Action | Result |
|---|---|---|
| 4 | `pydoc dagster.DataVersion` | Value type, no instance method to look up upstream's |
| 5 | `help(dagster.DataProvenance)` | Has `input_data_versions` but that's the OBSERVED versions, not look-up-on-demand |
| 6 | `dir(dagster)` filtered by 'Version' | All version-related public classes; none expose "look up upstream's" |
| What to do | No public API found → reformulate as Style A or read upstream file |

## Common gotchas this skill catches

(All sourced from real Brian sessions, May 2026.)

| You'd be tempted to write | Cheatsheet says | Why wrong |
|---|---|---|
| `from dagster._core.X import Y` | `avoid-private-imports.md` | Private path; breaks across versions |
| `tags["dagster/logical_version"]` | `tags-and-versions.md` | Renamed to `dagster/data_version` in 1.13 |
| `MultiPartitionsDefinition({a: ..., b: ..., c: ...})` (3 dims) | `partitions.md` | 1.13.3 supports max 2 dims |
| `from __future__ import annotations` + `@asset(... config: MyConfig)` | `future-annotations-incompat.md` | PEP 563 strings break Pydantic schema introspection |
| `MaterializeResult(...)` with constant hash + Style B | `data-version-and-staleness.md` | Silent staleness propagation break |

## Related

- [`docs/api-discovery-offline.md`](../../database/dagster-1.13.3/docs/api-discovery-offline.md)
  — full discovery walkthrough
- [`docs/avoid-private-imports.md`](../../database/dagster-1.13.3/docs/avoid-private-imports.md)
- The librarian's ROLE.md — overall personality contract
