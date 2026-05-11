---
name: smoke-test-lessons
description: Validate every learn/ lesson loads cleanly under Dagster 1.13.3. Run after editing lesson code, after dagster upgrade, or before claiming "lesson works". Catches import errors, decorator misuse, version-incompatible APIs.
---

<!-- all-might generated -->

# smoke-test-lessons — validate all learn/ lessons load

## When to use

- After editing any lesson's `asset.py` / `definitions.py` / `workspace.yaml`
- After upgrading `dagster` (e.g. 1.13.3 → 1.14.x)
- Before claiming a lesson is "working" — never trust unrun code
- After adding a new lesson

## What it checks

For each lesson, runs:

```bash
dagster definitions validate -m <pkg>     # single-module lessons
dagster definitions validate -w workspace.yaml   # multi-location (07, 08)
```

This **imports** the lesson's `Definitions(...)` and constructs the asset
graph — same path the webserver takes when loading a code location, but
without starting any UI or daemon. Catches:

- Import errors / `ModuleNotFoundError`
- Decorator misuse (`@asset` arg type errors, partition-def mismatches)
- API drift across Dagster minor versions (e.g. `MultiPartitionsDefinition`
  signature changes)
- Empty `__init__.py` failing to re-export `defs`
- `from __future__ import annotations` breaking Dagster reflection

It does **not** check:
- Whether asset bodies actually run successfully (no materialization)
- Whether UI behaves as the README describes
- Cross-lesson DAGSTER_HOME state interference

## How to run

```bash
source ~/dagster-venv/bin/activate
export DAGSTER_HOME=~/.dagster-tutor

cd ~/projects/personal-assistant         # or wherever the project is
TUTOR=personalities/dagster-expert/learn

run() {
  local dir="$1" arg="$2" result
  result=$(cd "$TUTOR/$dir" && dagster definitions validate $arg 2>&1)
  if echo "$result" | grep -q "All code locations passed"; then
    echo "PASS: $dir"
  else
    echo "FAIL: $dir"
    echo "$result" | grep -E "Error|^dagster\." | head -3 | sed 's/^/      /'
  fi
}

run "01-asset-and-materialize"        "-m hello"
run "02-deps-and-lineage"             "-m chain"
run "03-partitions"                   "-m by_corner"
run "04-runconfig"                    "-m configured"
run "05-failures"                     "-m flaky"
run "06-interrupt-rerun/6a-cancel"    "-m slow"
run "06-interrupt-rerun/6b-killed"    "-m killable"
run "06-interrupt-rerun/6c-restart"   "-m chunked"
run "07-cross-location"               "-w workspace.yaml"
run "08-complex-deps"                 "-w workspace.yaml"
```

Healthy output: 10× `PASS:`. Anything else, drop into the failing
lesson's dir and re-run with `--verbose` to see the full traceback:

```bash
cd $TUTOR/<failing-lesson>
dagster definitions validate <args> --verbose 2>&1 | less
```

## Known gotchas

These are the bugs the smoke test caught the first time it was run
(2026-05-09) — record them so future agents recognize the pattern:

| Symptom | Root cause | Fix |
|---|---|---|
| `Cannot annotate context parameter with type AssetExecutionContext` | `from __future__ import annotations` makes annotations strings; Dagster 1.13.3 reflection compares against the actual class | Drop the `__future__` import, OR leave `context` un-annotated |
| `Unable to resolve config type 'XConfig' to a supported Dagster config type` | Same `__future__` import breaks Pydantic-style Config schema introspection | Drop the `__future__` import |
| `MultiPartitionsDefinition supports 2 dims; got N` | 1.13.3 hard limit; not a bug | Collapse extra dims into a composite key, or drop to Route-A-style concrete assets |
| `No Definitions, Repository, ... found in <pkg>` | `<pkg>/__init__.py` is empty — Dagster looks at the package's top-level namespace and sees nothing | Add `from .asset import defs` to `<pkg>/__init__.py` |

## What to do if the test fails

1. Read the full traceback (`--verbose`).
2. Reproduce in a Python REPL: `python -c "from <pkg> import defs; print(defs)"`. If THAT fails the same way, the lesson code is broken irrespective of Dagster.
3. If it's a version-API issue (Dagster moved), check `dagster --version` matches lesson assumption (we pin 1.13.3).
4. Fix the lesson code, re-run smoke test, commit. **Don't claim "lesson works" without a green smoke test.**

## Why this exists

The first lesson set was written without smoke-testing. Brian
discovered 4/8 lessons failed validation only when he tried to run
Lesson 02. Three classes of bugs (annotation reflection, config
introspection, multi-partition arity) had been baked in for days.

The fix is structural, not just patching: any future changes to
`learn/*/` must run this skill before being declared done.
