# lab13 · LSF integration via Pipes

**Time**: 60 min · **Prerequisites**: lessons 09 (Pipes), 12 (scaling)

## What this demonstrates

Brian's TSMC environment uses IBM LSF for job scheduling. This
lesson wires Dagster asset bodies to bsub via a thin Python
wrapper, with Pipes for bidirectional event flow.

See `skills/lsf-executor/SKILL.md` for the full reference. This
lesson is the runnable proof.

## Pieces

```
13-lsf-integration/
├── pipelines/asset.py            ← Dagster @asset uses
│                                    PipesSubprocessClient + lsf_submit
├── scripts/python/lsf_submit.py  ← reusable wrapper:
│                                    assembles bsub flags, throttles
│                                    queue depth, forwards Pipes env
├── scripts/python/char_inner.py  ← runs on LSF node; opens
│                                    dagster_pipes, reports back
├── scripts/mock_lsf/bsub          ← local-dev bsub shim that
│                                    runs the inner command inline
└── _smoke.py                      ← end-to-end PASS in ~7s with mock
```

## Run it locally (with mock bsub)

```bash
cd ~/projects/.../learn/13-lsf-integration
python -m _smoke    # 6 partitions, 6 .lib + 6 .out logs
```

The smoke driver prepends `scripts/mock_lsf/` to `PATH` so
`bsub` resolves to the shim. On real LSF: just don't set PATH,
real bsub takes over.

## Run interactively

```bash
export PATH=$PWD/scripts/mock_lsf:$PATH
dagster dev -m pipelines
# open http://127.0.0.1:3000, click Materialize on a PVT cell
```

You'll see bsub-style log lines: `Job <NNNN> is submitted...`

## Real LSF migration

When you move to a real LSF host:
1. Drop the mock from PATH (don't `export PATH=...`)
2. Verify `bsub --help` resolves to real bsub
3. Possibly add site-specific defaults to `lsf_submit.py`:
   project code, queue, default walltime, memory tier

The asset code does NOT change.

## Six requirements covered

See `skills/lsf-executor/SKILL.md` for the verbose treatment.
Quick map:

| Requirement | Where it lives in code |
|---|---|
| (1) Log 紀錄 | `bsub -o/-e <NFS path>`; asset reads back into context.log |
| (2) 中斷機制 | `bsub -K` propagates Dagster terminate → bkill |
| (3) 狀態紀錄 | `bsub -K` exit code; or polling in async mode |
| (4) Env 繼承 | `lsf_submit.py --env-mode pipes-only/all/explicit` |
| (5) 平行跑 | Partitioned asset = N parallel bsubs (Dagster scheduling) |
| (6) Pending throttle | `lsf_submit.py --max-pending N` + Dagster queue concurrency |

## Related

- `skills/lsf-executor/SKILL.md` — the operator-facing reference
- `learn/09-real-flow/` — Pipes integration without LSF (base pattern)
- `learn/12-scaling/POSTGRES_MIGRATION.md` — at LSF scale you also need Postgres
