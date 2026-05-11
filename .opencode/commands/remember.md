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

Persist memory and maintain its quality. Two modes — pick the one that
matches the trigger context:

- **Record** — default for explicit "remember X" requests and the
  per-turn nudges. Capture a single observation now.
- **Reflect** — default for end-of-session, pre-compaction, and the
  L1 cap-audit nudge (`personalities/<active>/memory/.l1-over-cap`). Audit the whole memory
  surface for staleness, scope drift, and missing insights.

If unsure which mode applies, run **Record**; if `personalities/<active>/memory/.l1-over-cap`
exists or the trigger is a session-boundary event, run **Reflect**.

# Record

Record something worth persisting beyond this session.

## Reflect first, then record

Do not jump straight to "what feels important". That filter is biased
toward what you happened to notice and routinely drops lessons learned.
Run a short reflection sweep first; only then decide what to write.

1. **Replay the last few turns.** What did the user actually say? What
   did you do? Where were the course corrections, surprises, retries,
   or near-misses?
2. **Hunt for lessons learned** — these are the easiest to drop:
   - A misstep you recovered from (so the next session avoids it)
   - A non-obvious gotcha discovered while debugging
   - A user clarification that changed your understanding
   - A tool / command / API quirk you had to work around
   - A failed approach worth ruling out
   - An assumption that turned out to be wrong
3. **List candidates explicitly** before filtering. Write them out
   (mentally or in a scratch buffer) — every candidate gets considered,
   not just the headline result.
4. **Then filter.** Drop only items that are trivially re-derivable
   from code or already captured elsewhere. Small frictions still
   compound; a two-line debug-SOP note saves the next session 20
   minutes. Bias toward writing.
5. If the sweep comes up empty, skip the write — but always run the
   sweep first so a lesson is never silently filtered out.

## Decide the scope

Once you have something worth persisting, ask: **what is this about?**

| Scope | Location | Examples |
|-------|----------|----------|
| Project-wide | `MEMORY.md` (L1) | user preferences, env facts, active goals |
| Per-corpus knowledge | `personalities/<active>/memory/understanding/<workspace>.md` (L2) | architecture, key files, debug SOPs |
| Per-corpus personal state | `personalities/<active>/memory/<kind>/<workspace>.md` | open TODOs, shortcuts, ad-hoc notes |
| Historical / searchable | `personalities/<active>/memory/journal/<workspace>/…` (L3) | discoveries, decisions, session logs |
| Lesson learned (Mode-2) | `personalities/<active>/memory/lessons_learned/_inbox/<ts>-<user>.md` | observation flagged for curator review |

**Rule of thumb**: if it applies to one corpus only, put it under a
per-corpus file keyed by workspace name. Never dump per-corpus content
into `MEMORY.md` or into `personalities/<active>/memory/journal/general/`. When unsure,
prefer the **narrower** scope.

When a new `<kind>` of per-corpus content appears (e.g. a TODO list, a
list of preferred CLI flags, naming conventions), create
`personalities/<active>/memory/<kind>/<workspace>.md` on demand — follow the same
`<kind>/<workspace>.md` naming as `understanding/`. No new directory
needs to be declared up front.

## Routing across personalities

If this project has more than one personality, also ask: **which
personality** does this knowledge belong to? Bookkeeping the right
home for each fact is one of the reasons the user trusts the agent
as a secretary — don't shortcut it.

1. **Read the active personality** from `MEMORY.md`'s
   `> **Active personality**:` callout (loaded into your prompt
   every turn). That's the *default* per-corpus scope.
2. **Read each candidate personality's `STATUS.md`** (Active focus
   + Recent topics). The topic of the current observation usually
   lines up with one personality's recent activity.
3. **If the active personality matches the topic** → write under
   it as usual.
4. **If a *different* personality matches better** → don't silently
   write into the wrong one. Tell the user:

   > "This looks like it belongs to `<X>`, not the active `<Y>`.
   > Want me to switch first?"

   Wait for confirmation. If they say yes, **`Edit` `MEMORY.md`**
   to set the callout to `<X>`, then proceed with the write.
   Never auto-switch without asking.
5. **If the observation is genuinely cross-cutting** (a decision
   that affects multiple personalities), write the canonical fact
   to project-wide L1 (`MEMORY.md` → Key Facts) and add a one-line
   pointer in each relevant personality's
   `understanding/<topic>.md`. Don't duplicate the body — pointer
   only.

This routing rule is **soft** by design: you suggest, the user
confirms. Auto-switching without the user's intent is a debugging
trap.

### Switching mid-conversation

If the user says "switch to <name>" (or any equivalent — "act as
the reviewer", "let's use ops for this"), do this:

1. Verify `<name>` is in `MEMORY.md`'s Project Map.
2. `Edit` `MEMORY.md`, replacing the body of the
   `> **Active personality**:` callout with `<name>`.
3. Acknowledge: "Switched to `<name>`."
4. Read `personalities/<name>/STATUS.md` to load context.

That's the whole protocol. No state file, no CLI command, no
plugin parsing. The callout is loaded into every prompt via the
memory-load hook so the next turn already sees the new value.

## Lesson Learned (Mode-2 shared instance)

When this All-Might project is shared across a team via a common NFS
path (the **instance share** pattern), per-session writes to the
canonical L1/L2 surface get noisy fast. The `lessons_learned/_inbox/`
directory is the user-side write buffer for that case:

- **When to use:** during a shared-instance session you want to flag
  an observation for the personality's curator (a designated human or
  bot) to audit later, but it isn't yet authoritative enough to live
  in `understanding/` or `MEMORY.md`.
- **Where to write:**
  `personalities/<active>/memory/lessons_learned/_inbox/<ISO-8601>-<unix_user>.md`. The
  filename is per-user-per-timestamp, so concurrent reviewers never
  collide on the same file.
- **What to write:** short markdown with v1-style frontmatter:

```markdown
---
allmight_journal: v1
type: lesson_learned
submitter: <unix_user>
created_at: <ISO-8601>
tags: [<keywords>]
---
# <one-line title>

<a few sentences on what was observed and why future reviews
should know about it>
```

- **What NOT to put here:** stable knowledge belongs in
  `understanding/`; running session log belongs in `journal/`.
  `lessons_learned/_inbox/` is specifically the curator-audit queue.

The curator periodically walks `_inbox/`, decides keep / promote /
discard, and moves audited entries to `_reviewed/`. The framework
ships only the directory layout and this routing rule; the audit
loop itself lives outside All-Might (project-side script).

## What to remember

- **Corpus-specific knowledge**: architecture, patterns, key files, debug SOPs
- **Per-corpus personal state**: open TODOs, ad-hoc notes, shortcuts
- **User corrections**: "User clarified that X means Y"
- **Important decisions**: "Chose Redis over Memcached for pub/sub"
- **User preferences**: "User prefers concise answers"
- **Environment facts**: "Build requires Node 18+"

## How to execute

### 1. Update L2 understanding (primary, for knowledge)

If the observation is about a specific workspace, update or create
`personalities/<active>/memory/understanding/<workspace>.md`:

```markdown
## Architecture
(what you learned about the codebase structure)

## Key Files
(important files and what they do)

## Debug SOP
(how to diagnose common issues)
```

### 1b. Per-corpus personal state (create on demand)

If the observation is *mutable per-corpus state* rather than stable
knowledge — open tasks, preferred flags, personal shortcuts — write
to `personalities/<active>/memory/<kind>/<workspace>.md`. Example for TODOs:

```markdown
# <workspace> TODO

## Open
- [ ] <task> — <optional context / date>

## Done
- [x] <task> — <date completed>
```

Create the file (and its parent directory) on first use. Apply the
same pattern for other kinds you find yourself needing.

### 2. Append to L3 journal (for searchability)

Create a file in `personalities/<active>/memory/journal/<workspace>/` or `personalities/<active>/memory/journal/general/`.
Wrap it with **v1 frontmatter** so future offline analysis can query the
journal via `allmight memory export --format jsonl`. The freeform body
stays first-class; the frontmatter is mechanical:

```markdown
---
allmight_journal: v1
id: <ISO-8601 timestamp + short hash, e.g. 2026-04-18T10:32-a7f3>
type: discovery        # trajectory | reflection | discovery | decision | correction
workspace: <name>      # or: general
trigger: slash_remember
input: |
  <the user message that led to this, redacted of secrets>
tool_calls: []         # list of {tool, args, verdict: ok|drift|blocked}
output: |
  <your final response summary>
outcome_label: success # success | partial | failure | aborted
tags: [<keywords>]
supersedes: null       # id of an older entry this replaces, or null
created_at: <ISO-8601>
---
# <date> — <brief title>

<What you learned, in your own words.>
```

### 3. Update L1 MEMORY.md (only if portable)

**L1 is portable-only.** The test: "is this still true and useful no
matter which corpus I work on?" If no → it does NOT belong in
`MEMORY.md`.

Portable examples: user preferences, cross-cutting conventions, global
env facts, project-level goals, the project map of workspaces.

Corpus-specific knowledge, open TODOs, and work-in-progress state are
NOT portable and must go to L2 (`personalities/<active>/memory/understanding/<workspace>.md`)
or `personalities/<active>/memory/<kind>/<workspace>.md` instead. When unsure, write to the
narrower per-corpus location.

`MEMORY.md` is loaded every turn by a hook, so unbounded growth costs
every agent turn. A Stop hook audits the byte cap and — if exceeded —
writes `personalities/<active>/memory/.l1-over-cap` to nudge the next turn into the
**Reflect** mode below for triage.

## After remembering

1. Log what you remembered to `personalities/<active>/memory/usage.log` (scope tag enables
   the Reflect mode to audit drift):
```
<ISO-8601> remember scope=<project|workspace> workspace=<name|-> kind=<understanding|todos|journal|…> "<brief>"
```

2. **Keep STATUS.md current** — see `personalities/<active>/STATUS.md`.
   Update what changed:
   - **Always**: bump `last_activity` in the YAML frontmatter to now.
   - **If your write changed the personality's current focus**: rewrite the **Active focus** line.
   - **Add the topic to Recent topics** (keep ~5 entries, FIFO; drop the oldest).
   - **If you opened a new long-running thread** (a TODO you can't close in this session): add it to **Open threads**. If you closed one, remove it.

   STATUS.md is the rolling state surface that other sessions
   (and the human) consult to know "what is this personality
   currently doing?" without reading every journal entry. The
   project map in `MEMORY.md` may also have an "Active focus"
   column reflecting this — keep them consistent if both exist.

3. Run `smak ingest --config personalities/<active>/memory/smak_config.yaml` periodically to
   re-index the journal for `/recall` searches.

## What NOT to remember

- Trivial observations re-derivable from code
- Information already captured in sidecar enrichment
- Temporary debug notes

# Reflect

Structured self-reflection to maintain memory quality.

Run periodically (end of session, after major work, when the cap-audit
sentinel appears) to keep memory accurate and tidy.

## Steps

### 1. Review L1 — MEMORY.md

Read `MEMORY.md` at project root. Ask yourself:
- Is the Project Map still accurate? Any new workspaces to add?
- Are Active Goals still current? Remove completed ones.
- Any Key Facts that are stale or wrong?

Update directly if anything changed.

### 2. Review L2 — Understanding

For each workspace you worked on this session, read
`personalities/<active>/memory/understanding/<workspace>.md`. Ask:
- Did I learn new architecture details? Add them.
- Did I discover a debug SOP or gotcha? Document it.
- Is the Key Files section still accurate?

Create the file if it doesn't exist yet.

### 2b. Audit per-corpus scoping

List the files under `personalities/<active>/memory/` and check that each is scoped correctly
under the **scope-first** principle (see the **Record** section above):

- Anything in `MEMORY.md` that is really about *one* workspace?
  → Move it to the per-corpus file and leave at most a one-line
  pointer in `MEMORY.md` if the user truly needs it up front.
- Any `personalities/<active>/memory/journal/general/` entry that is really workspace-specific?
  → Move it under `personalities/<active>/memory/journal/<workspace>/`.
- Any ad-hoc per-corpus files you (or a past session) created —
  `personalities/<active>/memory/todos/<workspace>.md`, `personalities/<active>/memory/shortcuts/<workspace>.md`,
  etc.? → Confirm the name follows `<kind>/<workspace>.md` and the
  content is genuinely workspace-specific.

The goal: no matter what `<kind>` of personalized memory exists, it
lives under a consistent `personalities/<active>/memory/<kind>/<workspace>.md` path.

### 2c. L1 cap triage

Check for the cap-audit nudge sentinel:

```bash
cat personalities/<active>/memory/.l1-over-cap 2>/dev/null
```

If the file exists, MEMORY.md has grown past its byte cap. Triage
without waiting:

1. Read `MEMORY.md` line by line. For each line, classify it:
   - **Portable** (still useful in *any* corpus) → keep in L1.
   - **Corpus-specific** → move to
     `personalities/<active>/memory/understanding/<workspace>.md`.
   - **Open TODO / WIP** → move to `personalities/<active>/memory/todos/<workspace>.md` (or
     the matching `<kind>/<workspace>.md`).
2. Distill duplicates and stale bullets; keep the essence only.
3. Save `MEMORY.md`. The next Stop hook re-audits and removes
   `personalities/<active>/memory/.l1-over-cap` automatically when the body is back under
   cap.

**The cap never silently evicts anything** — this step is the only
place non-portable content leaves L1.

### 3. Log to L3 — Journal

Summarize what you learned this session as a journal entry in
`personalities/<active>/memory/journal/<workspace>/` or `personalities/<active>/memory/journal/general/`. Wrap it
with **v1 frontmatter** (see the **Record** section for the full
field list) so future offline analysis can query it; set
`type: reflection` and `trigger: slash_remember_reflect`.

### 4. Usage Review — Feedback Loop

Read `personalities/<active>/memory/usage.log` and analyze this session's activity:

- **Recalls**: How many `/recall` searches? Were results useful (`used` > 0)?
  - If a topic was recalled often → consider promoting it to L2 understanding
  - If recalls returned 0 results → knowledge gap, write it to journal
- **Remembers**: What categories? Are you remembering broadly or narrowly?
  - All in one workspace → good depth
  - Scattered across many → check if L1 project map needs updating
- **Enrichments**: Did you `/enrich` any symbols this session?
  - If you read code but didn't enrich → were there opportunities missed?
- **Stale L2**: List `personalities/<active>/memory/understanding/*.md` files. Any not loaded this
  session that haven't been updated in a while? Flag them.
- **Scope drift**: Count `remember` entries grouped by `scope=` and
  `kind=`. If per-corpus personal state is piling up under
  `scope=project` or `workspace=-`, the agent is being too generic —
  re-scope those entries to their workspace.

### 5. Generate Insights

Based on your usage review, write 2-3 actionable insights to
`personalities/<active>/memory/journal/general/` as a reflection entry.

### 6. Re-index (if needed)

If you added journal entries, re-index for `/recall`:
```bash
smak ingest --config personalities/<active>/memory/smak_config.yaml
```

### 7. Log the reflection

Append to `personalities/<active>/memory/usage.log`:
```
<ISO-8601> reflect insights=<N>
```

## When to reflect

- End of a productive session
- After completing a major task
- When the Memory Nudge reminds you
- When `personalities/<active>/memory/.l1-over-cap` appears
- When the user asks you to consolidate what you learned
