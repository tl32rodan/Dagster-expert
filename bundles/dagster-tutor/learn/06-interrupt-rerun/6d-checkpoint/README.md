# 6d · self-checkpoint pattern for Pipes-wrapped scripts

**Time**: 15-20 min · **Hardest sub-lab**

> This is the practical one. 6a-6c showed you what Dagster does
> when runs die — usually nothing nice for the work in flight.
>
> 6d shows the pattern that fixes it: have your Pipes-wrapped
> script **checkpoint its own progress**, so a re-run skips work
> the previous (killed) run already did.

## When this matters

A bsub'd Perl flow takes 30 minutes. You bsub it via Pipes. After
20 minutes the LSF host reboots. You re-run. Without checkpoints,
you redo the full 30 minutes. With checkpoints, the script reads
its own progress file on startup and skips the first 20 minutes
of work it already finished.

This is **NOT something Dagster gives you for free**. Dagster's
data-version mechanism is for staleness *across* runs of the
same asset; it doesn't help you resume *within* a run.

The pattern is on YOU as the script author.

## Setup

```bash
cd ~/projects/dagster-lab/lab6-interrupt-rerun/6d-checkpoint
dagster dev -m checkpointed
# UI: http://127.0.0.1:3000
```

This lab uses a Python script (not Perl, for simplicity — same
pattern applies). It "processes" 10 items, sleeping 2s each, and
writes a checkpoint file after each item.

## Walkthrough

### Step 1 · Materialize from scratch

Click **Materialize** on `processed_items`.

Run starts. Watch the run logs — you'll see `processed item N` log
lines from the Pipes script.

Let it complete (~22s). Asset is materialized. Check:

```bash
ls /tmp/dagster-lab-6d/
# checkpoint.json   (records "processed: 0..9")
# items/0.txt ... items/9.txt
```

### Step 2 · Materialize again (no checkpoint reset)

Click **Materialize** again WITHOUT cleaning the checkpoint dir.

Watch the logs:

```
fast-forward: items 0-9 already done per checkpoint. nothing to do.
```

Run finishes in ~1 second.

> The script read `checkpoint.json`, saw all 10 items already
> processed, did no work, returned a `MaterializeResult`. New
> `data_version` if you change the script's output formula —
> otherwise idempotent.

### Step 3 · Simulate a kill mid-run

```bash
# Reset the checkpoint
rm -rf /tmp/dagster-lab-6d/

# Materialize — let it run for ~10s, then SIGKILL the worker
ps -ef | grep 'execute_run' | grep -v grep
kill -9 <pid>
```

Run goes to `FAILURE` (after the heartbeat timeout, ~60s) or
stays `STARTED` orphaned. Doesn't matter; what matters is what's
on disk:

```bash
cat /tmp/dagster-lab-6d/checkpoint.json
# {"processed": [0, 1, 2, 3, 4]}     # got partway
ls /tmp/dagster-lab-6d/items/
# 0.txt 1.txt 2.txt 3.txt 4.txt       # 5 of 10 items done
```

### Step 4 · Re-execute

Click **Re-execute** (or Materialize again — same effect for this
asset).

Watch the logs:

```
checkpoint loaded: 5 items already processed
processing item 5
processing item 6
...
processing item 9
done
```

The script resumed from item 5. Total run time ~10s instead of 22s.

That's the win.

## The pattern, distilled

```python
# In your Pipes script (or asset body, doesn't have to be Pipes)

# 1. Define a checkpoint location somewhere durable
checkpoint = work_dir / "checkpoint.json"

# 2. On startup, read it
done = set()
if checkpoint.exists():
    done = set(json.loads(checkpoint.read_text())["processed"])
    log.info(f"checkpoint loaded: {len(done)} items already processed")

# 3. Process only items NOT already done
for item in items:
    if item in done:
        continue
    do_work(item)
    done.add(item)
    # 4. Update checkpoint after EACH item — atomic write
    tmp = checkpoint.with_suffix('.tmp')
    tmp.write_text(json.dumps({"processed": sorted(done)}))
    tmp.replace(checkpoint)   # atomic on POSIX

# 5. On clean completion, the checkpoint is just a record of total work
```

Notes on the atomic write: `tmp.replace(checkpoint)` is a POSIX
`rename(2)` — guaranteed atomic on the same filesystem. So
SIGKILL between writes can never leave a half-written
checkpoint.

## Now try

### Try 1 · Force a re-do without resetting the dir

What if you want to redo work even though the checkpoint says
"done"? Two options:

**Option A — delete the checkpoint**:
```bash
rm /tmp/dagster-lab-6d/checkpoint.json
```
Next run starts from scratch.

**Option B — bump the asset's `code_version`**:

Edit `checkpointed/asset.py`, change `code_version="1"` to `"2"`.
The next materialization runs from scratch (different code
version implies different work).

> See `lab4-config` for parameterizing this via RunConfig instead
> of editing source.

### Try 2 · Make the work fail at item 7 the first time

Edit the script to `raise RuntimeError("simulated failure")` when
processing item 7. Reset checkpoint and rerun. It dies.

Re-execute. It fast-forwards through 0-6 (already in checkpoint),
hits 7, dies again.

> The point: with checkpointing, you can fix the bug at item 7
> and re-execute. You re-do work for item 7 onward only, not
> 0-6.

### Try 3 · Multi-partition checkpoint

For a partitioned asset, each partition needs its own checkpoint
file (or use a single file keyed by partition). The natural
layout:

```
/tmp/dagster-lab-6d/
├── checkpoint-TT_25C_1V0.json
├── checkpoint-FF_125C_1V1.json
└── checkpoint-SS_m40C_0V9.json
```

Each partition's run reads/writes its own. Cross-partition
parallelism doesn't conflict.

## Common pitfalls

- **Non-atomic writes corrupt checkpoints**: if you write
  `checkpoint.json` directly with `json.dump`, a SIGKILL during
  write leaves a truncated JSON file. Next run can't parse it.
  **Always write to `.tmp` and rename**.
- **Side-effect files (items/*.txt) inconsistent with checkpoint**:
  If you write `items/N.txt` AFTER updating the checkpoint, a
  kill between can leave checkpoint claiming N is done but
  `items/N.txt` missing. Order matters: do the work, THEN update
  checkpoint. (Or: ensure work writes are themselves atomic.)
- **Checkpoints aren't garbage-collected**: an old run's
  checkpoint sits in `/tmp/...` forever. Either auto-clean (in
  the asset, on first call check for staleness via mtime), or
  put them under a per-run dir and let `/tmp` reaping handle it.
- **Don't checkpoint via Dagster's metadata**: `MaterializeResult`
  is only emitted on success. It's NOT a progress indicator for
  in-flight work. Use a dedicated file.
