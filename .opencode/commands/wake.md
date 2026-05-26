# /wake — flow-cartographer scheduler entry point

`/wake` runs ONE flow-cartographer tick. The personality + tick are
supplied explicitly by the caller (a `scheduled/*.md` task via the
chosen runtime, or a human dry-running a tick). Do NOT infer them from
conversation context or `MEMORY.md` — the scheduler/caller is the source
of truth.

## Signature

```
/wake flow-cartographer <plan|build|verify|reflect>
```

| Argument | Required | Values |
|----------|----------|--------|
| `personality` | yes | `flow-cartographer` (the only personality with the schedule capability) |
| `tick` | yes | `plan`, `build`, `verify`, `reflect` |

If `personality` is not `flow-cartographer`, or `tick` is missing /
unknown: **hard-stop** with the usage line. Do not fall back to a default.

## How to execute

1. Read **`personalities/flow-cartographer/skills/wake/SKILL.md`** — that
   file is the live Wake SOP; this command body is only a thin wrapper
   naming it as the entry point. If it is missing, abort and tell the
   user — do NOT improvise a routing table.
2. Execute the SOP steps in order: pre-flight → read handoff → re-read
   ROLE.md → route to `skills/<tick>-loop/SKILL.md` → run one tick →
   write handoff → termination check.
3. Report back: the primary artifact (the touched increment + its new
   status), or `outcome: noop` with the reason.

## When to invoke

- A `scheduled/*.md` task fires this once per tick (the task names which
  tick; `/wake` never picks).
- Manual dry-run: `/wake flow-cartographer plan` from a session, to see
  what a tick would produce.
- Never loop over ticks in a single invocation. One call = one tick.

## Boundary

`/wake` RUNS flow-cartographer (writes `flow-model/*`, the increment's
code, `STATUS.md`, the journal). It does not push to a remote and does
not edit the sibling `personalities/dagster-expert/…`.
