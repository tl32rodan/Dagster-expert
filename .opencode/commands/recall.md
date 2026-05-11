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

Pick up where you left off, and search past memories.

`/recall` is **not just** a journal search. Before running a query,
scan the per-corpus memory folders so you inherit any unfinished state
left from previous sessions (open TODOs, ad-hoc notes, shortcuts). The
SMAK journal search is the last step, not the first.

## Recall procedure

### 1. L1 — MEMORY.md (already in context)

`MEMORY.md` is injected every turn. Re-read the Project Map, User
Preferences, and Active Goals sections before assuming anything.

### 2. L2 — Per-corpus knowledge

For the workspace(s) relevant to the current task, read
`personalities/<active>/memory/understanding/<workspace>.md`.

### 3. Scan per-corpus folders generally (pick up where you left off)

List the `personalities/<active>/memory/` directory. For every subdirectory *other than*
`understanding/`, `journal/`, and `store/` (i.e. every per-corpus
`<kind>/` the agent or a past session has created), look for a file
matching the current workspace:

```bash
ls personalities/<active>/memory/
# for each <kind>/ present, check:
cat personalities/<active>/memory/<kind>/<workspace>.md 2>/dev/null
```

Typical kinds you may encounter:
- `personalities/<active>/memory/todos/<workspace>.md` — open TODOs; check `## Open` for
  anything left unfinished.
- `personalities/<active>/memory/shortcuts/<workspace>.md` — preferred CLI flags or aliases.
- `personalities/<active>/memory/notes/<workspace>.md` — ad-hoc workspace notes.

Any `<kind>` can exist — the agent creates them on demand via
`/remember`. Treat unknown kinds the same way: read, decide if
anything is unfinished, and proceed.

### 4. L3 — Journal (SMAK semantic search)

```bash
smak search "<query>" --config personalities/<active>/memory/smak_config.yaml --index journal --top-k 5 --json
```

Results from `personalities/<active>/memory/journal/` text files with file path, matched
content, and relevance score.

## When to recall

- At the start of a session touching a known workspace (steps 1-3).
- Before making assumptions about user preferences.
- When facing a problem that seems familiar.
- When the user asks "did we discuss X before?" (step 4).

## Switch hint — when results live under a different personality

If the active personality (from `MEMORY.md`'s
`> **Active personality**:` callout) is `<Y>` but the most relevant
`/recall` results are in `<X>`'s journal/understanding, surface
this to the user *before* showing the full results:

> "Top hits are from `<X>`, not the active `<Y>`. Switch to `<X>`
> for full context?"

This is a **hint, not an action**. You never auto-switch. If the
user accepts, `Edit` `MEMORY.md` to update the callout to `<X>`
and proceed with the recall in that personality's context.

If results are split roughly equally across personalities, present
them grouped by personality and let the user pick.

## After recalling

Log the recall to `personalities/<active>/memory/usage.log`:
```
<ISO-8601> recall "<query>" results=<N> used=<how many were relevant>
```
