---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-10T23:50:00+0800
tags: [smoke-test, validation, runtime, lesson-quality]
---
# `dagster definitions validate` doesn't catch runtime errors

## Observation

While walking Brian through Lesson 02, I (agent) wrote helper
code that passed `dagster definitions validate` cleanly but
crashed at runtime with `AttributeError: 'str' object has no
attribute 'key'`. The validation step only IMPORTS the module
and constructs `Definitions`; it doesn't INVOKE asset bodies.

Repeat occurrences in this session:
1. `tags = mat.tags or []; for tag in tags: tag.key` — wrong;
   tags is `dict[str, str]`
2. `tags.get("dagster/logical_version")` — wrong key name in 1.13
3. `extract_data_version_from_entry` from `_core` — runs but is
   a private import

All 3 passed `validate`. Only end-to-end materialize caught them.

## Implication for the tutor's lesson workflow

The `smoke-test-lessons` skill currently runs `dagster
definitions validate` against every lesson. That's necessary
but not sufficient.

For lessons that ship asset bodies relying on Dagster runtime
APIs (e.g. `context.instance.get_latest_materialization_event`,
helper functions, partition mapping nuances), we should
ADDITIONALLY run actual `materialize` calls — even just for ONE
representative partition through ONE representative chain.

The lesson 09 + 10 + 11 `_smoke.py` drivers already do this.
Lessons 02–08 don't.

## Curator action

Update `dagster-tutor/skills/smoke-test-lessons/SKILL.md`:
- Add a "Tier 2 — runtime smoke" section: run a single-partition
  materialize through each lesson's main chain, with a small test
  driver script
- Note: lessons 09, 10, 11 already ship `_smoke.py`; consider
  adding similar drivers for 02 (chain), 05 (flaky), 07
  (cross-loc), 08 (complex deps)
- Cross-reference the librarian's `data-version-and-staleness.md`
  cheatsheet for the specific `extract_*` and `tags.get` mistakes
  to watch for

Optionally: write a tutor-wide runner that takes the union of
all `_smoke.py` files and runs them sequentially. Useful for
"before-PR" gate.
