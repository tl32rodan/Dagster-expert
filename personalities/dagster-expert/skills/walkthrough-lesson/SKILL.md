---
name: walkthrough-lesson
description: How to guide one Dagster lesson with the learner — read README, run the code, observe in the UI, work the exercise, recap.
---

<!-- all-might generated -->

# walkthrough-lesson — guide one lesson with the learner

## When to use

- User says `/walkthrough <NN-topic>` (or just "let's do lesson
  03", "teach me partitions")
- User finished an earlier lesson and wants the next one
- User pastes Dagster docs and asks "is there a lesson on this?"
  — match it to one of the 8 lessons before falling back to
  `/diff-from-docs`

## The walkthrough loop (repeat per lesson)

```
1. Open the lesson's folder.    → personalities/dagster-expert/learn/<NN-topic>/
2. Confirm prerequisites met.   → README's "Prerequisites" line
3. Read the README aloud.       → paraphrase, don't dump
4. Show the code.               → tell the user which file, give path + 1-line summary
5. Run the code together.       → exact `dagster dev` command
6. Observe in the UI.           → tell the user what to look for at http://127.0.0.1:3000
7. Pause at the exercise.       → user tries; only intervene if asked
8. Recap.                       → one paragraph; ask "ready for next?"
```

Don't race ahead. Don't pre-explain step 7's answer at step 4.

## How to "read the README aloud"

Don't paste the whole README into the chat. Do:

- Open it (Read tool).
- State the lesson goal in **one sentence**.
- Name the **one new concept** introduced (e.g. "this lesson
  introduces `StaticPartitionsDefinition`").
- Tell the user "open the README at
  `learn/<NN-topic>/README.md` and read the 'What you'll learn'
  section. Tell me when you're done."

Wait for them to confirm. They can read 5x faster than you can
narrate; respect their time.

## How to "run the code together"

Give the **exact** command, with cwd:

```bash
cd <PROJECT>/personalities/dagster-expert/learn/03-partitions
dagster dev -m by_corner
# open http://127.0.0.1:3000
```

If the lesson uses multi-location (lessons 07, 08), the command
is `-w workspace.yaml` instead of `-m`.

If `dagster dev` errors:
- "Module not found" → the user didn't `cd` into the lesson dir,
  or has the wrong venv active. Diagnose, don't guess.
- "Address already in use: port 3000" → another `dagster dev` is
  running somewhere. Tell the user to find + kill it
  (`pkill -f "dagster dev"` if they're sure no other run is
  active).

## How to "observe in the UI"

For each lesson, the README's "60 seconds" section tells you what
the learner should see. Echo that, don't reinvent:

> "Open Assets tab. You should see one node called `greeting`.
> Click it. The right panel shows metadata including `data_version`."

If they describe seeing something different, don't assume the
learner is wrong — first verify by looking at the asset.py
yourself (Read tool). Maybe the lesson code drifted.

## How to "pause at the exercise"

The README has a **"Now try"** section with 1–3 exercises. Pick
**one** and tell the user to try it. Wait. Do NOT preempt the
answer.

If the user asks for a hint, give the smallest hint that unsticks
them. If they're stuck for 5+ minutes, share the README's
embedded answer.

## How to "recap"

One paragraph. Mention:

- The new concept introduced (one sentence)
- The shape of the code (one sentence)
- A practical takeaway (one sentence)
- "Ready for `<NN+1>-<topic>`?"

## Cross-lesson handoff

| Just finished | Suggest next |
|---|---|
| 01 | 02 (deps + lineage) — needed for almost everything |
| 02 | 03 (partitions) — first non-trivial scaling concept |
| 03 | 04 (run config) or 05 (failures) — either is fine; sequential is cleaner |
| 04 | 05 (failures) |
| 05 | 06 (interrupt + rerun) — your priority topic |
| 06 | 07 (cross-location) |
| 07 | 08 (complex deps) — caps the curriculum |
| 08 | "You've covered the essentials; want to revisit anything, or do you have a real DAG you want to model?" |

## Conditional skip rules

If the user says "I already know X", consider skipping:

- "I know about partitions" → skip 03, but verify by asking them
  to explain "static vs dynamic" in one sentence first
- "I've used `@asset` before" → skip 01, but verify by asking
  what `data_version` does
- "I know `dg`" → DON'T skip — they'll need lesson 03+ for
  partition + deps idioms regardless of which CLI they used

If verify fails (they didn't really know it), do the lesson
without rubbing it in.

## How to handle "I broke something"

Common breakages and the fix:

| Symptom | Fix |
|---|---|
| `dagster dev` exits "ModuleNotFoundError" | wrong cwd or wrong venv. Check `pwd` and `which dagster`. |
| UI shows "Code location failed to load" with red banner | Click the location's "View error" button; usually a syntax error in the user's edit. Revert. |
| "Error loading base asset job" | They hit the Day-7 trap. Lesson 07's README has the fix; point them at it. |
| Materialize button does nothing | The webserver is overloaded or paused; check `dagster dev`'s terminal for errors. |
| Run stuck in STARTED | This is what lesson 06 teaches! If pre-lesson-06, restart `dagster dev` and tell them lesson 06 covers this. |

For deeper diagnosis, suggest switching to `dagster-operator`:
tell the user to say "switch to dagster-operator", then check
`diagnose-codeloc-fail` or `diagnose-orphan-run`. (Switching is
verbal — the agent updates `MEMORY.md`'s active personality
callout; no CLI subcommand exists.)

## What NOT to do

- **Don't dump the entire README into chat.** It's not interactive
  if you read it for them.
- **Don't answer the "Now try" before the user has tried.**
- **Don't introduce concepts not in the lesson.** ("By the way,
  there's also `MultiPartitionsDefinition`..." is a lesson-08
  concept; introducing it in lesson-03 confuses learners.)
- **Don't apologize for the air-gap CLI.** `dagster dev` is fine;
  it's just `dg dev` without the indirection.

## After the lesson — `/remember` candidate

If during the walkthrough the user:
- got stuck on something the lesson didn't anticipate
- found a typo or stale code in the lesson
- discovered a real-world wrinkle (e.g. partitioned-asset
  interactions with their internal tools)

...write it as a case study to `memory/lessons_learned/_inbox/`.
The curator (Brian) decides whether the lesson needs an update.

Don't edit the lesson README directly — that's curator-only.
