---
allmight_journal: v1
type: lesson_learned
submitter: tl32rodan
created_at: 2026-05-10T23:45:00+0800
tags: [materializeresult, value, style-a, iomanager, public-api]
---
# `MaterializeResult.value=` is the public Style-A IOManager handoff

## Observation

While walking lesson 02, I (the agent) initially proposed a
Style B + `dagster._core.definitions.data_version.extract_data_version_from_entry`
hack to get upstream data_version into a downstream's hash. Brian
correctly pushed back ("`_core` is private — what's the public
API?").

The clean public path is **Style A with `MaterializeResult(value=...)`**:

```python
@asset
def upstream() -> MaterializeResult:
    payload = b"..."
    return MaterializeResult(
        value=payload,                            # IOManager stores
        data_version=DataVersion(_digest(payload)),
    )

@asset
def downstream(upstream: bytes) -> MaterializeResult:
    output = transform(upstream)                  # value flows through IOManager
    return MaterializeResult(
        value=output,
        data_version=DataVersion(_digest(output)),
    )
```

## Key public-API facts that were unclear

1. `MaterializeResult` has a public `value=` field for IOManager
   handoff. This is the modern (1.13+) shape.
2. The default IOManager pickles `value` to local filesystem and
   feeds it to downstream as a function argument (matching the
   parameter name to the upstream asset key).
3. `data_version` propagation is automatic when downstream's hash
   includes the bytes received as the function argument.

## Why the librarian cheatsheet should make this prominent

The cheatsheet's `style-a-vs-b.md` mentions Style A but the
example doesn't use `MaterializeResult.value`. New consumers
(human or LLM) may default to "Style A means just `return
output`" and miss that you can also return MaterializeResult
WITH a value, getting both metadata reporting AND IOManager
handoff in one return.

## Curator action

Update `style-a-vs-b.md`:
- Update the Style A example to use
  `MaterializeResult(value=..., data_version=..., metadata=...)`
- Add a note: "Style A producer can return `MaterializeResult`
  with both `value=` (for IOManager) and metadata; you don't
  have to choose between propagation and reporting."

Update `data-version-and-staleness.md`:
- The "Two correct patterns" section already shows Style A but
  could call out `MaterializeResult.value=` as the explicit field
  responsible for IOManager handoff.

Add to `avoid-private-imports.md` as a cross-reference:
- "Looking up upstream `data_version` from event log? You
  probably want Style A with `MaterializeResult.value=` instead."
