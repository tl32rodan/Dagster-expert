---
name: wake
description: >
  flow-cartographer wake-up SOP. The single entry point for every
  scheduled tick AND every interactive session. Runs pre-flight, reads
  the handoff, re-reads ROLE.md, routes to one tick loop, runs it, then
  writes the handoff. One wake = one tick = one action.
---

# Wake SOP — the standard procedure for every wake

You are the entry point for flow-cartographer. Whether the scheduler
fired a tick or a human opened a chat, you run THIS procedure first,
mechanically, ticking each step out loud ("Step N: done"). You do not
improvise and you do not start work before Step 2 (read handoff).

This is the **ralph-loop**: the same SOP runs on every wake; each wake
advances the conversion by exactly one increment; the loop repeats on
the schedule until `CONVERSION.md`'s success criteria are met.

## Signature

```
/wake flow-cartographer <plan|build|verify|reflect>
```

| Argument | Required | Values |
|----------|----------|--------|
| `tick` | yes | `plan`, `build`, `verify`, `reflect` |

If `tick` is missing or unknown: **hard-stop**. Print
`ERROR: /wake flow-cartographer requires one of: plan build verify reflect`
and exit. Do NOT guess a default. If a human started a chat without a
tick arg, infer the tick from the handoff's `next_action` (Step 2) and
state which tick you are running and why.

---

## Step 0 — Pre-flight (mechanical, tick the boxes)

`Read personalities/flow-cartographer/PRE_FLIGHT_CHECKLIST.md` and tick
every box out loud. The boxes that gate work:

- `$FLOW_SRC` set and is an existing dir (all ticks) — else REFUSE with
  the `setenv`/`export` remediation.
- `CONVERSION.md` exists and has no `[PLACEHOLDER]` in Mission/Success
  (all ticks) — else this is a setup turn: help the user fill the
  charter, then stop.
- `$DAGSTER_HOME`, venv, `dagster --version == 1.13.3` (build + verify
  only) — else REFUSE.

If a gating box fails, STOP at the refusal. Do not proceed.

---

## Step 1 — Re-read ROLE.md

```
cat personalities/flow-cartographer/ROLE.md
```

Re-read it every wake even if you think you remember it. The file is
the live spec; your in-context memory may be stale or pruned. This is
the cheapest insurance against drift.

---

## Step 2 — Read the handoff (this is where you learn "where am I")

Read these three, in order, and state in one line what the last tick
did and what this tick must do:

1. `personalities/flow-cartographer/STATUS.md`
   — note `active_increment`, `open_threads`, and the `next_action` baton.
2. `personalities/flow-cartographer/flow-model/_plan.yaml`
   — the ledger. Find increments by `status`.
3. `personalities/flow-cartographer/CONVERSION.md`
   — the charter: goal, partition strategy, constraints, success criteria.

> **You may not skip Step 2.** A weak model with no memory of prior
> context resumes ONLY from these files. If `next_action` and the
> ledger disagree, trust the ledger (`_plan.yaml`) — `next_action` is a
> hint, the ledger is authoritative — and note the discrepancy in
> `_operations.log`.

Pick the increment this tick will act on (see each loop's selection
rule). For `build`/`verify`, that is normally the increment named in
`next_action`.

---

## Step 3 — Route to the tick loop (read by path, do not re-implement)

Read and follow the matching loop skill. These live under
`personalities/flow-cartographer/skills/` and are intentionally NOT
registered under `.opencode/skills/`, so read them BY PATH.

| tick | Read and follow |
|---|---|
| `plan` | `skills/plan-loop/SKILL.md` |
| `build` | `skills/build-loop/SKILL.md` |
| `verify` | `skills/verify-loop/SKILL.md` |
| `reflect` | `skills/reflect-loop/SKILL.md` |

Run exactly one. One wake = one tick = one action.

---

## Step 4 — Write the handoff (this is where the NEXT tick learns "where am I")

After the loop runs (fully, partially, or as a no-op), update ALL of:

1. **Ledger** `flow-model/_plan.yaml` — set the touched increment's
   `status` and any fields the loop produced.
2. **Ops log** `flow-model/_operations.log` — append ONE line:
   ```
   <ISO-ts> <tick> <increment-id> <action> <one-line result>
   ```
   If you can't describe what you did in one line, you did too much.
3. **STATUS.md**:
   - bump frontmatter `last_activity` to now (ISO).
   - set `active_increment` to the increment in flight (or `none`).
   - append one `recent_ticks` entry (FIFO, max 5; drop oldest):
     ```yaml
     - date: YYYY-MM-DD
       tick: build            # plan|build|verify|reflect
       increment: <id or '-'>
       outcome: planned | built | verified | blocked | proposed | noop
     ```
   - update `open_threads` (add blockers / parked questions; remove
     resolved).
   - rewrite the one-line **`next_action`** baton: the single most
     useful thing the next wake should do (e.g.
     `verify increment a3 (built, awaiting self-check)` or
     `build increment a4 (next planned in topo order)`).
4. **Journal** `memory/journal/<flow-name>/<YYYY-MM-DD>-<tick>-<short-title>.md`
   — the decision/verdict with citations.

**No handoff update = the tick didn't happen.** If the loop produced
nothing, still write a `noop` recent_ticks entry and an ops-log line
saying why.

---

## Step 5 — Termination check (the ralph-loop stop condition)

Evaluate `CONVERSION.md`'s success criteria against the ledger:

- **Not all met** → leave `next_action` pointing at the next increment.
  The schedule will wake you again. (Normal case.)
- **All met** → write `flow-model/digest/<date>-conversion-complete.md`
  (3 lines: what's converted, what smoke proves it, what's intentionally
  out of scope), set `_plan.yaml` top-level `conversion_status: met`,
  and set `next_action: polish-only (criteria met YYYY-MM-DD; user:
  extend CONVERSION.md, retire the flow, or accept polish-only)`. Do NOT
  stop the schedule yourself — the user decides.

In `polish-only` mode, `build`/`verify` ticks no longer pick new
increments; `reflect` keeps running (re-validate, propose amendments).

---

## Failure escalation

| Situation | Action |
|---|---|
| ledger empty and tick is `build`/`verify` | Run `plan` first (or tell the user to). Do not invent increments. |
| ambiguity about the flow's behavior | Park it in `flow-model/_open_questions.yaml`; mark the increment `blocked`; do NOT guess. |
| any loop raises an unhandled error | Append one line to `flow-model/_open_questions.yaml` under `errors:`; do not swallow it silently. |
| unknown / missing tick arg | Hard error (see Signature); for interactive, infer from `next_action` and state it. |

## What wake does NOT do

- It does not pick the tick (the scheduler / `next_action` does).
- It does not run more than one tick per call.
- It does not push to a remote (publishing is the deployment's concern).
- It does not edit `personalities/dagster-expert/…` (sibling territory).
