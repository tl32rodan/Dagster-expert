# lab19 · AutoMaterializePolicy on partitioned assets

**Time**: 60 min · **Prerequisites**: lessons 03 (partitions),
16 (hooks + auto-materialize basics), 17 (cross-partition
incremental), 06 (interrupt/rerun)

> 💡 Lesson 17 was manual: you re-materialize upstream, the UI
> tells you which downstream partition is stale, then you click
> "Materialize" on the stale one. Lesson 19 is the same setup,
> but the **daemon** clicks "Materialize" for you on every tick
> when an EAGER asset has a stale partition.

## The four-asset shape

```
raw_corner ─► mid_corner_eager ─► final_corner_eager
                              └─► final_corner_lazy
```

Same 4-corner partition def on all four assets:
`ff_125c`, `tt_25c`, `ss_m40c`, `ss_125c`. Per-asset policy:

| Asset | Policy | Behavior |
|---|---|---|
| `raw_corner` | none | You materialize this by hand |
| `mid_corner_eager` | `AutoMaterializePolicy.eager()` | Daemon re-runs each stale partition ASAP |
| `final_corner_eager` | `AutoMaterializePolicy.eager()` | Same, cascading from mid |
| `final_corner_lazy` | `AutoMaterializePolicy.lazy()` | Goes stale, daemon waits for a request |

## Setup

```bash
cd 19-auto-materialize-partitioned
dagster dev -m reactive
# UI: http://127.0.0.1:3000
```

`dagster dev` starts BOTH the webserver and the daemon. The
daemon is what makes auto-materialize fire. If you accidentally
run `dagster-webserver` alone, auto-materialize won't tick.

In the UI's **Auto-materialize** sidebar (left nav), confirm the
daemon is "running" and policies are "enabled" for the EAGER
assets. New deployments default to enabled; older ones may need
a one-time toggle.

## Walkthrough

### Step 1 · backfill `raw_corner`

```bash
dagster asset materialize -m reactive --select raw_corner --partition ff_125c,tt_25c,ss_m40c,ss_125c
```

Within ~30s (one daemon tick), the daemon should:

1. Detect `mid_corner_eager` has 4 stale partitions → launch
   a run for each (or a backfill, depending on tick batching).
2. Once those complete, detect `final_corner_eager` has 4
   stale partitions → launch runs for those.
3. `final_corner_lazy` is also stale but is NOT auto-run.

UI: **Runs** tab shows ~8 auto-launched runs ("Launched by
auto-materialize policy"). **Assets** view: three of the four
assets fully green; `final_corner_lazy` stays empty / stale.

### Step 2 · bump one upstream partition

Edit `reactive/asset.py`'s `raw_corner`:

```python
payload = f"raw__{key}__rev=1".encode()   # change rev=1 to rev=2
```

Reload code locations.

### Step 3 · re-materialize ONE upstream partition

```bash
dagster asset materialize -m reactive --select raw_corner --partition ff_125c
```

### Step 4 · watch the daemon do the rest

Within ~30s:

- `mid_corner_eager[ff_125c]` auto-materializes.
- Then `final_corner_eager[ff_125c]` auto-materializes.
- Other 3 partitions of `mid_corner_eager` and `final_corner_eager`
  are NOT touched (only `ff_125c` was stale).
- `final_corner_lazy[ff_125c]` is now stale; daemon does NOT
  run it.

This is the cross-partition incremental promise from lesson 17,
now with **no human clicks** for the downstream EAGER chain.

### Step 5 · trigger the lazy asset

To get `final_corner_lazy[ff_125c]` to actually materialize:

```bash
dagster asset materialize -m reactive --select final_corner_lazy --partition ff_125c
```

Or click "Materialize" on it in the UI. LAZY means "fresh only
when someone asks" — it never auto-runs on upstream change alone.

## eager() vs lazy() in partitioned context

**EAGER**:
- Daemon evaluates every partition of the asset on every tick.
- Any partition that is stale → enqueue a run for it.
- If many partitions go stale at once (e.g. you re-materialize
  all 4 of upstream), expect a batch of runs.

**LAZY**:
- Daemon never auto-launches on stale alone.
- Only fires when a downstream EAGER asset depends on it AND
  needs it (cascaded request).
- Use this for "expensive to compute, only needed sometimes":
  reports, aggregations, expensive downstream views.

Rule of thumb: **EAGER along the critical path**, **LAZY for
side-branches and reports**. In an AP flow that means most
step-N → step-(N+1) edges are EAGER; the final sign-off report
that consumes everything might be LAZY (only run when explicitly
asked).

## What the daemon actually looks at

Every ~30s the daemon walks asset graph nodes that have
`auto_materialize_policy=AutoMaterializePolicy.eager()`. For each
such asset, for each partition, it asks: "is this partition
stale or never-materialized AND should it run now?" If yes, it
enqueues a run via the same machinery as the UI's "Materialize"
button.

Conditions for "should run":
1. Stale or missing.
2. Upstream(s) are fresh — no point running until input is ready.
3. Policy rules pass (default rules cover the common cases; you
   can add custom rules to e.g. skip during off-hours).

The daemon log (`$DAGSTER_HOME/logs/daemon.log` or `dagster
dev`'s stdout) prints what each tick decided. If auto-materialize
isn't firing when you expect, that log is the first thing to
check.

## Cross-asset, cross-partition: the production point

Combine this with lesson 18 (cross-location): an EAGER asset in
`lib_upper` that depends on a partitioned asset in `lib_lower`
will auto-rebuild its matching partition whenever `lib_lower`
publishes a new version. Multi-team automation, zero clicks.

That's the production target for the "incremental change event"
painpoint in the two-tier framing (`memory/understanding/why-two-tier.md`):

- Tier 1 (Dagster) knows about partitions, versions, and
  cross-asset deps.
- Tier 2 (LSF / scripts) does the heavy compute inside one
  asset.
- An auto-materialize policy + correct `data_version`
  propagation = the system reacts to "this branch changed" with
  exactly the right partitions of exactly the right downstream
  steps.

## Pitfalls

- **Auto-materialize doesn't fire** — daemon not running.
  Confirm with `dagster dev` startup logs:
  `Started Dagster daemon process`. If you used
  `dagster-webserver` alone, the daemon is absent. Restart with
  `dagster dev`.
- **Daemon ticks but nothing runs** — the EAGER policy isn't
  enabled. UI → Auto-materialize sidebar → toggle on per asset.
  Older Dagster versions ship with policies disabled until
  explicitly enabled.
- **All 4 partitions auto-run when you only changed one
  upstream** — `data_version`s are constant-hashes (the bug from
  lesson 17c). Without per-partition `data_version` movement,
  the daemon can't isolate which partition actually changed and
  fires runs for all of them. Fix the upstream's hash to fold
  in per-partition content (Style A or Style B as in 17a/17c).
- **Auto-materialize launches THE SAME partition over and over
  in a loop** — your asset's `data_version` is non-deterministic
  (e.g. includes a timestamp). Each materialization records a
  new version → downstream sees stale → re-runs → new version
  → ... Fix: hash content, not time.
- **LAZY asset never materializes even when I want it to** —
  expected. LAZY does NOT run on upstream change. Materialize
  it by hand, or attach an EAGER consumer that pulls it.
- **Daemon log says `evaluated X assets in 0.05s, requested 0
  runs`** every tick despite stale partitions — check the
  policy is on the *asset*, not just imported. The decorator
  must include `auto_materialize_policy=AutoMaterializePolicy.eager()`.

## What this lesson is NOT covering

- **Custom auto-materialize rules** (skip during business hours,
  cap concurrent auto-runs). 1.13.3 has `AutoMaterializeRule.*`
  helpers; see the cheatsheet under
  `personalities/dagster-expert/database/dagster-1.13.3/docs/`
  if you need to author one. For most production cases
  `eager()` / `lazy()` are enough.
- **`AutomationCondition`** — Dagster's declarative-automation API
  (`automation_condition=AutomationCondition.eager()`) has
  **superseded `AutoMaterializePolicy` since Dagster 1.8**, and it
  ships in our pinned 1.13.3 — `AutoMaterializePolicy.eager()/.lazy()`
  still runs there but is **deprecated** (emits a warning). This
  lesson keeps the older API only because `AutomationCondition` is
  **not yet in this offline corpus** (`database/dagster-1.13.3/docs/`,
  0 hits), so the verify "API must exist in the corpus" check would
  reject it. To migrate: `/enrich` the corpus with the
  `AutomationCondition` symbols first, then swap
  `auto_materialize_policy=AutoMaterializePolicy.eager()` →
  `automation_condition=AutomationCondition.eager()`.
- **Sensors that trigger backfills** — covered in lesson 15.
  Auto-materialize is the declarative cousin of sensors for
  the "fresh upstream" trigger.

## What to try next

Combine lessons 17 + 18 + 19 in your head: cross-partition,
cross-location, auto-materialized. That's the entire
"incremental change event" surface area for the AP flow. The
canonical production reference for all three is `demo/scale-lib/`
in this repo.
