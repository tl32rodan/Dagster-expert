---
name: lsf-executor
description: Run Dagster asset bodies on IBM LSF (bsub-managed cluster). Covers job submission, log capture, status tracking, env inheritance, parallel fan-out, and queue-depth throttling. The recommended pattern is asset-body bsub via dagster_pipes — not a custom RunLauncher.
---

<!-- all-might generated -->

# lsf-executor — submit Dagster asset work to IBM LSF

## When to use

- You have an HPC/LSF cluster (typical TSMC EDA flow): jobs go
  through `bsub`, scheduled via fair-share, output lands on
  shared NFS
- Each asset body wants to delegate the heavy lift to LSF, not
  run inline on the Dagster host
- You need cluster-aware retry / cancel / pending throttle
  semantics

## When NOT to use

- Your work fits on one host — just use Dagster's default
  in-process executor
- You need `dagster-daemon`-level scheduling on the LSF side —
  that requires a custom `RunLauncher` (much more work; out of
  scope for this skill)

## Architecture — asset-body bsub via Pipes

```
   ┌───────────────────────────────┐
   │  Dagster host (login node)    │
   │  • dagster-webserver          │
   │  • dagster-daemon             │
   │  • @asset bodies run here as  │
   │    Python subprocesses        │
   └─────────────┬─────────────────┘
                 │  bsub -K wrapper.py ...
                 ▼
   ┌───────────────────────────────┐
   │  LSF compute node             │
   │  (one per partition / asset)  │
   │  • wrapper.py opens           │
   │    dagster_pipes              │
   │  • reads/writes via shared    │
   │    NFS                        │
   └─────────────┬─────────────────┘
                 │
                 ▼  results, logs, materialization events
            (read back by asset body on Dagster host)
```

Why this shape:
- Dagster's scheduler stays in-process (no LSF involvement at
  the Dagster level — Dagster only sees a subprocess)
- Each @asset = one bsub call. Failure isolation is at the
  asset partition level
- Pipes does the Dagster integration: structured events, log
  forwarding, MaterializeResult reporting

## The `bsub` flag reference you'll actually use

```bash
bsub \
    -K                                  # synchronous: block until done; returns job's exit code
    -J <name>                           # job name (visible in bjobs)
    -q <queue>                          # queue (e.g. normal, long, high_mem) — ASK YOUR IT
    -o <stdout.log>                     # capture stdout to shared NFS file
    -e <stderr.log>                     # capture stderr separately
    -W <hh:mm>                          # wall-time limit (job killed past this)
    -n <slots>                          # parallel slots (cores) requested
    -R "rusage[mem=4096]"               # memory request in MB
    -R "select[type==X86_64]"           # host selection predicates
    -P <project>                        # project code for billing
    -env "all"                          # inherit ALL env vars from submitter
    -env "VAR1=value1, VAR2=value2"     # OR: explicit subset (no "all")
    -cwd /shared/work/<run_id>          # working directory on compute node
    python3 wrapper.py --arg value      # actual command (last position)
```

Discover your site's allowed queues:
```bash
bqueues                                 # list all queues + their config
bqueues -l <queue>                      # queue limits / fairshare / users
```

Find available resource specs:
```bash
bhosts -l                               # host inventory
lsload                                  # current host loads
```

## Status tracking — `bjobs`

```bash
bjobs <jobid>                            # state of one job
bjobs -a                                 # all jobs (including DONE/EXIT in last hour)
bjobs -l <jobid>                         # detail (resource usage, host, etc.)
bjobs -o "jobid stat exit_code user" -w  # custom output format
bjobs -u $USER -p                        # only pending jobs for me
bjobs -u $USER -r                        # only running jobs for me
```

States:
- `PEND` — pending in queue, awaiting resources
- `RUN`  — running on a compute node
- `DONE` — completed with exit code 0
- `EXIT` — completed with non-zero exit code
- `PSUSP / USUSP / SSUSP` — suspended

## Cancel — `bkill`

```bash
bkill <jobid>                            # send SIGTERM, then SIGKILL after 30s
bkill -s SIGINT <jobid>                  # specific signal
bkill -r <jobid>                         # force kill (escalate sooner)
bkill 0                                  # kill ALL my pending+running jobs (CAREFUL)
```

When you use `bsub -K` (synchronous), Dagster's terminate sends
SIGTERM to the local bsub process. **bsub propagates this to the
underlying LSF job automatically** — no manual bkill needed in
the asset body. You can verify post-mortem with `bjobs -a`.

If you use asynchronous bsub + bjobs polling (no `-K`), you MUST
register a signal handler in the asset body to `bkill` the job
on terminate.

## The 6 Brian requirements — concrete recipes

### (1) Log 紀錄

```python
@asset
def my_lsf_asset(context: AssetExecutionContext,
                  pipes_subprocess_client: PipesSubprocessClient):
    run_id = context.run.run_id
    log_path = Path(f"/nfs/logs/{run_id}.log")
    err_path = Path(f"/nfs/logs/{run_id}.err")

    result = pipes_subprocess_client.run(
        command=[
            "bsub", "-K",
            "-J", f"dagster_{context.asset_key.to_user_string()}",
            "-o", str(log_path),
            "-e", str(err_path),
            "-env", "all",
            "python3", str(WRAPPER), "--out-dir", str(out_dir),
        ],
        context=context,
    ).get_materialize_result()

    # Slurp the LSF log into Dagster's run logs
    if log_path.exists():
        for line in log_path.read_text().splitlines():
            context.log.info(f"[LSF stdout] {line}")
    return result
```

The LSF log file goes to shared NFS (so the Dagster host can
read it after the job exits). Pipes events are SEPARATE — they
flow via the `DAGSTER_PIPES_MESSAGES` path (also NFS) and appear
as proper event-log entries.

### (2) 中斷機制

With `bsub -K`:
- Dagster terminate → local bsub process gets SIGTERM
- bsub auto-propagates SIGTERM → bkill underlying LSF job
- LSF job exits → bsub returns nonzero → asset body raises
- Dagster marks run CANCELED

No additional code in the asset body. **Verify** this is your
site's bsub behavior — older LSF versions may differ.

Defensive alternative (works on any LSF version):

```python
import signal, subprocess, os

@asset
def my_asset(context, pipes_subprocess_client):
    proc = subprocess.Popen(["bsub", "-K", "-J", "...",
                             "python3", "wrapper.py"],
                            preexec_fn=os.setsid)
    try:
        # Wait, but allow SIGINT/SIGTERM to propagate
        proc.wait()
    except (KeyboardInterrupt, SystemExit):
        # Get the LSF job id and bkill
        # (parse bsub's output line "Job <12345> is submitted")
        subprocess.run(["bkill", lsf_jobid], check=False)
        proc.terminate()
        raise
```

### (3) 狀態紀錄

Pattern A — **synchronous**: bsub returns exit code, no polling needed.

Pattern B — **asynchronous + poll**:

```python
import re, subprocess, time

def submit_async(cmd: list[str]) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True, check=True)
    m = re.search(r"Job <(\d+)> is submitted", r.stdout)
    return m.group(1)

def wait_for_lsf(jobid: str, poll_interval: int = 30) -> tuple[str, int]:
    while True:
        r = subprocess.run(
            ["bjobs", "-a", "-o", "stat exit_code", "-noheader", jobid],
            capture_output=True, text=True,
        )
        parts = r.stdout.split()
        state = parts[0]
        if state in ("DONE", "EXIT"):
            exit_code = int(parts[1]) if len(parts) > 1 else 0
            return state, exit_code
        time.sleep(poll_interval)
```

Report state transitions via Pipes log messages so users see the
PEND → RUN → DONE timeline in Dagster's run UI.

### (4) Env 繼承

Default `bsub` does NOT inherit env. Three patterns:

| Pattern | Use |
|---|---|
| `bsub -env all` | Inherit ALL env vars from submitter — risky if there are large/sensitive vars |
| `bsub -env "DAGSTER_PIPES_CONTEXT=$DAGSTER_PIPES_CONTEXT,DAGSTER_PIPES_MESSAGES=$DAGSTER_PIPES_MESSAGES,DAGSTER_HOME=$DAGSTER_HOME"` | Whitelist what Pipes needs + your app's vars |
| Source a profile script in wrapper: `bsub ... bash -c "source /shared/profile.sh && exec wrapper.py"` | Inherit nothing; rebuild env from a known profile |

For Dagster Pipes integration, you MUST pass at least:
- `DAGSTER_PIPES_CONTEXT`
- `DAGSTER_PIPES_MESSAGES`

If using shared NFS for the messages file (typical with LSF),
both env vars contain a NFS path the LSF compute node can reach.

### (5) 平行跑

Dagster handles this at the partition level naturally:
- Partitioned asset with N partitions = N `materialize` calls
  = N parallel bsub submissions (subject to `max_concurrent_runs`)
- Each bsub returns a separate LSF jobid; LSF queue schedules
  them according to fair-share

Tune `dagster.yaml`:

```yaml
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 50          # how many @asset bodies run concurrently
    tag_concurrency_limits:
      - key: dagster/concurrency_key
        value: lsf_char
        limit: 20                    # at most 20 in-flight bsubs of "lsf_char" type
```

Tag the assets:

```python
@asset(tags={"dagster/concurrency_key": "lsf_char"})
def char_asset(...): ...
```

### (6) Pending throttle — "DAG 輪到他再去排隊"

The KEY insight: this is NOT an LSF problem; it's a Dagster
scheduling problem. **As long as each @asset's body only bsubs
ITS OWN partition's work**, you never pre-stage thousands of
LSF jobs.

Dagster's scheduling guarantees:
- An @asset materialization is triggered when (and only when)
  upstream deps are ready
- The asset body THEN bsubs. Not before.

So the LSF queue depth = number of currently-in-flight Dagster
asset runs (capped by `max_concurrent_runs`), not the total DAG
size.

Additional safety belt — check pending depth before bsub:

```python
import subprocess

MAX_PENDING = 100

def wait_for_queue_slot():
    while True:
        r = subprocess.run(
            ["bjobs", "-u", os.environ["USER"], "-p", "-noheader"],
            capture_output=True, text=True,
        )
        n_pending = sum(1 for _ in r.stdout.splitlines() if _.strip())
        if n_pending < MAX_PENDING:
            return
        time.sleep(60)
```

Call `wait_for_queue_slot()` at the top of each asset body if
your team has strict queue-depth policy.

## Implementation — `lsf_submit.py`

Reusable wrapper that all assets call. See
`learn/13-lsf-integration/scripts/python/lsf_submit.py` for
the full file. The asset body becomes:

```python
@asset(partitions_def=pvt_partitions, tags={"dagster/concurrency_key": "lsf_char"})
def liberate_run(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
):
    keys = context.partition_key.keys_by_dimension
    return pipes_subprocess_client.run(
        command=[
            "python3", str(LSF_SUBMIT),
            "--job-name", f"char_{keys['corner']}_{keys['volt_temp']}",
            "--queue", "normal",
            "--walltime", "01:00",
            "--memory-mb", "8192",
            "--log-dir", "/nfs/logs",
            "--",                                # everything after is the inner command
            "python3", str(CHAR_WRAPPER),
            "--corner", keys["corner"],
            "--vt", keys["volt_temp"],
        ],
        context=context,
    ).get_materialize_result()
```

The `lsf_submit.py` handles bsub flag assembly, env passing,
PEND → RUN → DONE state forwarding, queue throttling, and
returning the inner command's exit code.

## Common gotchas

- **NFS clock skew** — if Dagster host and LSF compute nodes
  see different times, Pipes events arrive "in the future" and
  Dagster's UI sorts wrong. Run NTP on all nodes.
- **`bsub -K` exit code semantics** — exit code is the JOB's
  exit code, not LSF's. If LSF rejects the job (queue closed,
  invalid spec), bsub returns nonzero — check stderr to
  differentiate "queue rejected" from "your code failed".
- **Pipes messages file race** — both Dagster host and LSF
  node write to the same file. Use an LSF-host-local temp file,
  then rsync/copy back at job end if NFS is too slow. (Pipes
  default expects shared FS.)
- **bkill on a PEND job is fast; on RUN it depends** — RUN
  jobs receive SIGTERM, then SIGKILL after 30s by default. If
  your code traps SIGTERM and ignores it, bkill stalls.
- **`-env all` with secrets** — if you have `DAGSTER_PG_PASSWORD`
  in env, `-env all` copies it to every LSF node. Use the
  whitelist pattern for sensitive vars.

## Testing without a real LSF

For local dev where you don't have LSF: see
`learn/13-lsf-integration/scripts/mock_lsf/` for a `bsub`
shim that prints the LSF command, runs it locally, and returns
realistic output. Drop it on `PATH` before `dagster dev`:

```bash
export PATH=/path/to/mock_lsf:$PATH
dagster dev -m pipelines
```

The asset bodies bsub normally; the mock executes inline. Same
code works against real LSF when you flip `PATH`.

## What this skill does NOT cover

- **Custom LSFRunLauncher** — making each Dagster RUN itself a
  bsub'd job. Possible (subclass `RunLauncher`) but adds
  complexity and the asset-body pattern usually covers the need.
- **Site-specific LSF policies** — your IT may require specific
  `-P project`, `-q queue`, walltime limits, fair-share account.
  Ask your LSF admin; bake into `lsf_submit.py` defaults.
- **GPU jobs** (`-gpu "num=1:mode=exclusive"`) — same pattern,
  different flags.
- **Job arrays** (`bsub -J "job[1-100]"`) — when you'd want to
  submit thousands of similar jobs as one LSF entity. Better
  expressed in Dagster as a partitioned asset where each
  partition is one bsub; let Dagster handle the array semantics.

## Related

- `learn/13-lsf-integration/` — runnable demo using mock bsub
- `learn/09-real-flow/` — Pipes integration without LSF
  (the base pattern this builds on)
- `learn/12-scaling/POSTGRES_MIGRATION.md` — at LSF scale you
  almost certainly need Postgres-backed Dagster instance
