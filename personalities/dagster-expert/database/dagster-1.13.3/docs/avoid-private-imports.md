# `dagster._core.*` — private modules to avoid

**Tested against Dagster 1.13.3.**

## Rule

**Never import from anywhere whose path starts with `_`.**

| Private path (DON'T) | Public alternative |
|---|---|
| `from dagster._core.X import Y` | `from dagster import Y` (if exposed) |
| `from dagster._core.definitions.data_version import extract_data_version_from_entry` | Don't query metadata; redesign with Style A or filesystem-read (see `data-version-and-staleness.md`) |
| `from dagster._core.events.log import EventLogEntry` | `from dagster import EventLogEntry` |
| `from dagster._core.storage.X import Y` | Mostly nothing public — Brian's flow shouldn't touch storage layer directly |
| `from dagster._internal.X import Y` | Same — there's no public version on purpose |

## How to check if something is public

```python
# Public top-level symbols
import dagster
public = [a for a in dir(dagster) if not a.startswith('_')]
print('DataVersion' in public)        # True → public
print('extract_data_version_from_entry' in public)   # False → DON'T use
```

The `_is_public = True` attribute on classes is Dagster's
explicit marker:

```python
import dagster
print(getattr(dagster.EventLogRecord, '_is_public', False))   # True
```

## Why this rule

Private modules:
- Can be renamed/moved/deleted in any minor version (1.13.3 → 1.13.4 → 1.14)
- Are not covered by Dagster's deprecation policy (which only
  applies to public API)
- Often have undocumented invariants — using them safely
  requires reading source

Brian hit this directly: his lesson 02 helper used
`dagster._core.definitions.data_version.extract_data_version_from_entry`,
which works in 1.13.3 but isn't part of the public API contract.
A 1.14 release could remove it tomorrow with no breakage notice.

## What to do when you NEED something not public

1. **Re-examine the design.** Often "I need to access private X"
   means you're fighting Dagster's intended pattern. Style B
   `_core` reach-ins almost always indicate "should be Style A".

2. **Wrap it explicitly with a single-file shim** (last resort):

   ```python
   # _dagster_compat.py
   """
   Private-API shim for Dagster <1.14. Pinned 1.13.3.
   Review on every Dagster upgrade.
   """
   try:
       from dagster._core.definitions.data_version import extract_data_version_from_entry
   except ImportError as e:
       raise ImportError(
           "extract_data_version_from_entry was removed/renamed. "
           "Check Dagster CHANGELOG and update this shim."
       ) from e
   ```

   Now Dagster upgrades fail loud at import, not silently at
   runtime in production.

3. **File a Dagster GitHub issue / discussion** asking for the
   public API you actually need (when you have internet — i.e.
   Brian on the dev side, not air-gap users).

## Detection — find sneaky `_core` imports

```bash
# In any Python source tree
grep -rn "from dagster\._" --include='*.py' .
grep -rn "import dagster\._" --include='*.py' .
```

Output should be empty. Anything not empty needs a justification.

For air-gap reviewer: this could be a hard rule in the
`reviewer` personality's `basic-practice` ruleset.

## Related

- [`api-discovery-offline.md`](api-discovery-offline.md) — how to
  find the right PUBLIC API
- [`data-version-and-staleness.md`](data-version-and-staleness.md)
  — concrete case where the private import was wrong solution
