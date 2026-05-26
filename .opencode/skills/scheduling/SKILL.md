# Scheduling — register flow-cartographer's ticks with a runtime

Ported from the All-Might `schedule` capability. This project uses it to
run `flow-cartographer`'s four ticks (`plan`/`build`/`verify`/`reflect`)
on a cadence. The declarative task files already exist at
`personalities/flow-cartographer/scheduled/*.md`; this skill is how you
materialise them to an actual runtime.

## When to invoke

"schedule", "automate", "run the cartographer loop", "set up the
ticks", "cron", "every night". If the user only says "just schedule
it", ask two questions first:

1. **Persistence** — must it outlive the current session?
2. **Machine-on** — OK if it only fires while the box is awake?

## Runtime matrix

| Runtime | When to pick | Persistence | Min interval | State lives in |
|---|---|---|---|---|
| **External cron / systemd** (likely default on the air-gap box) | tcsh air-gap workstation, no OpenCode session running; the box is always on | OS scheduler | 1 min | crontab / unit files |
| **OpenCode + `opencode-scheduler` plugin** | the user runs OpenCode and wants cadence to outlive a session | OS scheduler unit | 1 min | `~/.config/opencode/scheduler/scopes/<id>/jobs/*.json` |
| **Claude Code `/loop`** | throwaway polling during one CC session (dry-run a few ticks) | session-only | 1 min | session JSON |

On the TSMC air-gap box (tcsh, no internet, possibly no OpenCode), the
realistic answer is **external cron** calling a thin wrapper that runs
one tick prompt. Document the crontab lines; do not auto-install them.

## Slug discipline (non-negotiable)

Every job uses `am-<personality>-<task>`. For this project that is
exactly the four declared slugs:

```
am-flow-cartographer-plan      cron "0 18 * * 1"          (weekly Mon 18:00)
am-flow-cartographer-build     cron "0 20-23,0-7 * * *"   (hourly off-peak :00)
am-flow-cartographer-verify    cron "30 20-23,0-7 * * *"  (hourly off-peak :30, after build)
am-flow-cartographer-reflect   cron "0 5 * * 0"           (weekly Sun 05:00)
```

The `am-` prefix marks the job as All-Might-owned. Reject any proposed
slug without it. The cron + slug + body are already written in each
`scheduled/<tick>.md` frontmatter — read those, do not re-invent them.

## Each tick's prompt body (self-contained)

When a tick fires, a FRESH session starts — no conversation context
survives. The prompt for every tick is the same one line:

```
/wake flow-cartographer <tick>
```

…which routes through `personalities/flow-cartographer/skills/wake/SKILL.md`
(pre-flight → read handoff → run one tick → write handoff). The handoff
files ARE the cross-session memory; that is why no context needs to
survive.

**`permission_mode: allow` is required** for build/verify/plan/reflect
(declared in each `scheduled/*.md`). The loop must Edit/Write/Bash and
commit; the scheduler's default `deny` would hang the run. This is an
explicit, understood override (the loop edits code) — surface it to the
user when you register the jobs.

## If using `opencode-scheduler` (OpenCode only)

Detect it in `.opencode/opencode.json` (`"plugin": ["opencode-scheduler"]`).
Absent → tell the user once to add it + restart; do NOT auto-edit
`opencode.json`. Tools: `schedule_job({slug, cron, prompt,
timeoutSeconds, permissionMode})`, `list_jobs()` (filter by `am-`),
`delete_job({slug})`, `run_job_now({slug})` (manual test fire). Read the
slug/cron/timeout/permission from each `scheduled/*.md` frontmatter.

## If using external cron (air-gap default)

For each tick, add a crontab line that activates the venv and runs the
one-line wake prompt through the box's agent CLI in this repo dir, e.g.:

```
0 20-23,0-7 * * *  cd /abs/path/to/Dagster-expert && <agent-cli> "/wake flow-cartographer build" >> ~/cartographer.log 2>&1
```

Replace `<agent-cli>` with the air-gap agent runner. Keep `verify` 30
min after `build`. Document the lines; the user installs them.

## Anti-patterns

- **Don't run build and verify in the same job.** Verify is a separate,
  different-framing gate; collapsing them defeats the verify-gate.
- **Don't schedule from a fresh `allmight init`.** Scheduling is opt-in.
- **Don't omit the `am-` prefix.** Collides with user jobs.
- **Don't push from a tick.** Ticks commit locally; publishing is the
  deployment's concern.
- **Don't sub-minute.** Floor is 1 min.

## Verifying it works

`run_job_now({slug: "am-flow-cartographer-plan"})` (opencode-scheduler)
or run the crontab line by hand once. Confirm the tick wrote the handoff
(`STATUS.md` `last_activity` bumped, a `recent_ticks` entry, an
`_operations.log` line). No handoff update = the tick didn't run.
