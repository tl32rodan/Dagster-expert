# lab17 · observer-mode Tier 1

**Time**: 60 min · **Prerequisites**: lessons 15 (sensors), 16
(hooks + auto-mat); recommended: read `memory/understanding/why-two-tier.md`

## What this demonstrates

The **observer-mode adoption pattern** — phase 1 of Brian's
two-tier rollout (per `why-two-tier.md`). Tier 1 (Dagster)
watches an existing AP system that keeps running unchanged.
Each AP step completion becomes a Dagster materialization
event, giving you a full execution record (TSMC AP painpoint
#3) with **zero risk** — Tier 1 schedules nothing.

```
AP (still in production, unchanged)        Tier 1 (Dagster observer)
─────────────────────────────────────      ─────────────────────────────
runs ap_step.pl                            @observable_source_asset
   ↓ writes /tmp/.../step1/out/*.lib       ticks ~30s, hashes the .done
   ↓ touches /tmp/.../step1/.done          file's mtime → DataVersion

                                           Sensor + AutoMaterializePolicy.eager()
                                           react when source moves: trigger
                                           Tier 1 recorder asset

                                           Recorder reads AP output (read-
                                           only), folder_digest, writes JSON
                                           snapshot. UI shows: run history,
                                           folder digest per AP step.
```

After ~1 week of observer-mode in production:
- Diff digests across runs → which steps actually changed
- Identify steps with noisy digests (need exclusion rules)
- Build trust with the team before letting Tier 1 actively schedule

## Why this is the right way to start

The two-tier rationale (`memory/understanding/why-two-tier.md`)
identifies three TSMC AP painpoints. Observer-mode **fully
solves #3** (no execution record) with zero invasiveness. It
also **partially probes #2** (no incremental) because you can
diff Tier-1 records across runs and see which steps' folder
digests actually moved.

What it doesn't address:
- #1a (fine-grain per-PVT): Tier 2's job
- #1b (cross-library): Phase 3
- Active scheduling: Phase 2 (Step take-over)

Don't conflate these. Observer is observer.

## Run it

### Smoke

```bash
source ~/dagster-venv/bin/activate
cd ~/projects/.../learn/17-observer-mode
python -m _smoke
```

The smoke driver:
1. Simulates AP runs by writing files + touching the `.done` marker
2. Materializes the Tier 1 recorder per step
3. Verifies the data_version moves when the AP output changes

Expected output:
```
>>> Round 1: AP completes step0 + step1 + step2
    AP touch files written + output folders populated
>>> Tier 1 records each step (observer mode)
    step0: digest=abc12345... files=2
    step1: digest=def67890... files=2
    step2: digest=ghi98765... files=2
>>> Round 2: AP re-runs step1 with new payload
    AP touch + output rewritten
>>> Tier 1 re-records step1
    step1 round 1 digest: def67890...
    step1 round 2 digest: jkl54321...
    DIGESTS DIFFER — Tier 1 caught the AP rerun
=== PASS ===
```

### Interactive — see it in the UI

```bash
dagster dev -m pipelines
# open http://127.0.0.1:3000
# enable the ap_touch_sensor in the sensors page

# In another shell, simulate AP runs:
mkdir -p /tmp/dagster-17-mock-ap/step1/out
echo "first" > /tmp/dagster-17-mock-ap/step1/out/result.txt
touch /tmp/dagster-17-mock-ap/step1/.done
# Wait ~30s — sensor fires, Tier 1 records step1

# Simulate AP rerun
echo "second" > /tmp/dagster-17-mock-ap/step1/out/result.txt
touch /tmp/dagster-17-mock-ap/step1/.done
# Wait ~30s — sensor fires again, new materialization with different digest
```

UI behavior:
- **Assets tab** → 3 source assets (`ap_*_touch`) + 3 Tier 1
  recorder assets, grouped under `ap_observed` and `tier1_records`
- **Sensors tab** → `ap_touch_sensor`; can start / stop / inspect cursor
- **Each recorder's Materializations** → snapshot history with
  data_version + folder_digest metadata

## Files in this lesson

```
17-observer-mode/
├── README.md            — this file
├── _smoke.py            — end-to-end PASS (~3s, mock AP)
├── workspace.yaml
└── pipelines/
    ├── __init__.py
    └── asset.py         — sources + recorders + sensor
```

## Two pieces working together

### `@observable_source_asset` per AP step
The source asset's `DataVersion` is the touch-file's mtime. Each
observation tick (default ~30s daemon) re-reads the mtime and
emits a new DataVersion if it changed. This is the change-event
signal that makes "incremental" possible.

### Sensor + `AutoMaterializePolicy.eager()` (belt-and-suspenders)
The Tier 1 recorder asset has BOTH:
1. `auto_materialize_policy=AutoMaterializePolicy.eager()` — the
   daemon auto-rematerializes when an upstream source moves.
2. An explicit `@sensor` polling the touch file — redundant when
   the daemon is healthy, but useful for smoke runs / explicit
   diagnostics. Cursor tracks last-seen mtime per step.

In production, the sensor is the safety net if auto-mat is
disabled or daemon is recovering. Stop it if you trust auto-mat.

## Mapping to TSMC AP today

To wire this against real AP:

1. Find the AP touch file convention for each step (typically
   `<workdir>/.done` or `<workdir>/<step>.complete`)
2. Map each into `_ap_touch_file(step)` of this lesson
3. Map each AP output dir into `_ap_output_dir(step)`
4. Reuse the recorder pattern; the smoke driver becomes
   unnecessary (real AP triggers it)
5. Possibly move the folder_digest computation to the AP host's
   step-completion hook (see `demo/scale-lib/CONTRACT.md` § "Who
   computes the folder digest") so Dagster avoids the NFS stat
   cost

## Common gotchas

- **Daemon must run** for sensors + auto-mat to fire. `dagster
  dev` has one; production needs `dagster-daemon run`.
- **mtime resolution = 1 second** on most filesystems. Two AP
  reruns within the same second won't be distinguished by mtime
  alone. If your AP completes faster than that, hash content
  instead of mtime.
- **NFS mtime skew** between hosts can cause the source asset to
  observe stale or future mtimes. Run NTP on AP hosts + Dagster
  host.
- **Touch file but no output**: AP touches `.done` before the
  output is fully written → Tier 1 fires too early, sees partial
  folder. AP should either touch *after* output complete (best)
  or Tier 1 should sleep N seconds before reading (fragile).

## Next steps after observer-mode

After ~1 week of observer-mode running in production with no
surprises:

- Pick the step with the highest incremental-rerun pain. Build a
  Tier 1 *active* asset for it (no longer just observer): Tier 1
  invokes the AP script via subprocess, AP touch file is now an
  output of Tier 1's run. Other AP steps that watch this touch
  file still see it; no AP-side change needed.
- Iterate per-step. Each take-over is independently revertible —
  just disable the Tier 1 asset and let the legacy AP path run.

That's Phase 2 of the rollout. See
`memory/understanding/why-two-tier.md`.

## Related

- `memory/understanding/why-two-tier.md` — the rationale this
  lesson implements
- `learn/15-sensors/` — sensor mechanics (recommended pre-read)
- `learn/16-hooks-automaterialize/` — `AutoMaterializePolicy.eager()`
- `demo/scale-lib/pipelines/source_observers.py` — the
  `pvt_manifest`/`cell_list` precedent for `observable_source_asset`
- `demo/scale-lib/CONTRACT.md` § "Who computes the folder digest" —
  central vs node-side digest tradeoff
