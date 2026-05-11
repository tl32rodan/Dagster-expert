# Air-gap API discovery technique

**Tested against Dagster 1.13.3 + Python 3.10+.**

How to find the right Dagster public API when you have no
internet. Air-gap agents (Kimi K2.5, MiniMax M2.5, offline
Claude) MUST use this technique instead of generating from
training memory.

## Search order — strictest first

```
1. cheatsheet (../docs/<topic>.md)
   ↓ if not found
2. example (../examples/<NN>_*.py)
   ↓ if not found
3. python -m pydoc dagster.<Name>
   ↓ if not found
4. python -c "from dagster import X; help(X)"
   ↓ if not found
5. python -c "import dagster; print([x for x in dir(dagster) if not x.startswith('_')])"
   ↓ as a last resort
6. inspect source: less $(python -c 'import dagster; print(dagster.__file__)')
```

**Never skip to step 6 without exhausting 1-5.** Source-code
reading is fragile (you might land in private internals).

**Never reach into `dagster._core.*` / `_internal.*` / `_private.*`** —
see `avoid-private-imports.md`.

## Step-by-step

### Step 1: cheatsheet

Look at `INDEX.md` first. Topics are organized by user question
(e.g. "how do I parameterize a run?" → `runconfig.md`).

If the question matches an entry, **read the entry, follow its
example, you're done**.

### Step 2: examples

If the cheatsheet topic exists but is too high-level, read the
runnable example. Examples are smoke-tested against the pinned
Dagster version, so they're guaranteed to import + load.

### Step 3: pydoc

If no cheatsheet entry exists for the API you need:

```bash
# Top-level public API
python -m pydoc dagster.MaterializeResult
python -m pydoc dagster.AssetExecutionContext
python -m pydoc dagster.RetryPolicy

# Submodule public API
python -m pydoc dagster.PipesContext
```

Output includes the constructor signature, all public methods,
and the docstring with `Args:` describing each parameter.

### Step 4: help() in REPL

```python
python
>>> import dagster
>>> help(dagster.DataVersion)
>>> help(dagster.MaterializeResult.__init__)   # specific method
```

Same content as pydoc, interactive.

### Step 5: dir() + filter

When you don't know the class name:

```python
import dagster

# All public top-level symbols
public = [a for a in dir(dagster) if not a.startswith('_')]

# Filter by keyword
matches = [a for a in public if 'Version' in a]
# → ['DataVersion', 'DataVersionsByPartition']

# Public classes that Dagster explicitly marks
import dagster
classes = [
    a for a in public
    if isinstance(getattr(dagster, a), type)
    and getattr(getattr(dagster, a), '_is_public', False)
]
```

The `_is_public` flag is Dagster's own marker for "this is
intentionally public" — useful for distinguishing public types
from incidentally-exposed names.

`Annotated[X, 'public']` annotation pattern in constructors is
another marker — check `EventLogRecord` for an example.

### Step 6: source (last resort, only if 1-5 fail)

```bash
DAGSTER_DIR=$(python -c "import dagster, os; print(os.path.dirname(dagster.__file__))")
ls $DAGSTER_DIR/                    # public-ish modules at top level
ls $DAGSTER_DIR/_core/              # private — DON'T import from here
```

Read code only to confirm what `pydoc` already told you. **Never
commit code that imports from anywhere starting with `_`.**

## What to do if NO public API exists for what you need

1. **Stop.** Don't smuggle in a `_core` import.
2. **Tell the user**: "No public API found for X. Options: (a)
   reformulate the design — usually a Style A vs Style B
   confusion (`data-version-and-staleness.md`), (b) file a
   `/remember` entry to flag the gap and ask if it's worth
   asking upstream Dagster, (c) accept the limitation and
   work around it."
3. If the limitation is real, document the workaround in a
   cheatsheet entry so the next agent doesn't repeat the search.

## Worked example — finding `data_version` propagation API

Question: "How do I read upstream's data_version inside an asset?"

Step 1: `cheatsheet/data-version-and-staleness.md` exists →
**answers the question (don't query metadata; use Style A or
read the upstream file directly)**. Done.

If the cheatsheet didn't exist, the search would proceed:

Step 3: `pydoc dagster.DataVersion` — shows it's a value type,
no method to read upstream's. Step 5: `dir(dagster)` filtered
by 'Version' — shows `DataVersion`, `DataVersionsByPartition`,
`DataProvenance`. `pydoc dagster.DataProvenance` shows it has
`input_data_versions: Mapping[AssetKey, DataVersion]` — but
that's the consumer's view of what it observed, not "look up
upstream's data_version on demand". So no public API exists for
the original ask. Step "what to do": reformulate as Style A, log
the question.

## Why this matters

The default LLM failure mode for a fast-moving library is to
**generate confident-but-wrong code from outdated training
memory**. Brian has hit this 3 times in one Lesson 02 session
(tag keys, MultiPartitions arity, `_core` reach-in). The cost
each time is debug iterations.

This discovery sequence makes verification cheap (`pydoc`
returns in <1s) and the wrong-API window narrow.

## Related

- [`avoid-private-imports.md`](avoid-private-imports.md) — what
  not to import
- [`tags-and-versions.md`](tags-and-versions.md) — example of
  what cross-version research looks like in this corpus
