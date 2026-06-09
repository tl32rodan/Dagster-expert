# Architecture choice — sync-execution (v1) vs async-execution (v2)

> **For**: anyone starting a new flow, or maintaining an existing one and
> reconsidering its execution model.
>
> **TL;DR**: two execution modes co-exist as supported framework
> products. The choice is **not** "old vs new" — both are first-class.
> Pick by run-count, task-duration, and LSF-queue-depth. The decision
> tree in §3 takes < 60 seconds.

## 0. The two modes

| | **Mode A — Sync-execution** (v1) | **Mode B — Async-execution** (v2) |
|---|---|---|
| Marketing name | "in-Dagster execution" | "outsourced execution" / "Execution Fabric" |
| Whitepaper | `FIVE_LAYER_WHITEPAPER.md` | `EXECUTION_FABRIC_WHITEPAPER.md` |
| Reference impl | `spec_dagster/` (D1+D2 verified, PR #18) | `execution_fabric/` (Phase 1 plan in `PHASE_1_PLAN.md`; not yet built) |
| Hand-rolled comparison | `examples/liberate-char/converted/` (the example this design was extracted from) | — |
| Dagster's role | lineage **+** execution | lineage **only** |
| Asset body | does the computation (in-process Pipes / launcher-bsub'd run worker) | dispatches the computation and returns immediately |
| Dagster run lifetime | = computation duration (minutes–hours) | seconds (dispatch only) |
| Execution truth | Dagster event log | external status DB (Phase 1: SQLite + file lock; Phase 2: PostgreSQL) |
| Sensor model | reconcile sensor (`trigger: reconciliation`) OR `AutomationCondition.eager()` cascade (`trigger: automation`, default in spec.yaml as of 2026-06-08) | two sensors: **dispatch** + **harvest** (`report_runless_asset_event`) |
| LSF dispatch unit | the run worker process (Dagster run = 1 bsub) | one computation script (not a run) |
| Failure semantics | Dagster native (run retry, from-failure, UI status) | self-built (idempotency key + 3 fault contracts, whitepaper §6) |
| UI semantics | Dagster UI green-light = computation succeeded | Dagster UI green-light = **dispatched only**; real progress in self-built UI |
| Implementation complexity | low | ~2× (status DB + worker + harvest sensor + faults) |
| Implementation effort | D1+D2 ≈ 8d, done | Phase 1 ≈ 8.5d, not started |

## 1. The essential distinction (not sync vs async)

The colloquial framing is "synchronous vs asynchronous"; the more
useful framing is **what does a successful Dagster run mean?**

- **Mode A**: a successful Dagster run = the computation finished
  successfully. Dagster's event log is the single source of truth for
  both lineage and execution state.
- **Mode B**: a successful Dagster run = the computation was
  **dispatched** to the execution layer. Whether the computation
  succeeded is a separate fact, recorded in the external status DB and
  reflected back into Dagster lineage by the harvest sensor.

This distinction propagates into every concrete difference in the table
above. The two modes are not "earlier vs later iteration of the same
design"; they are different products of different trade-offs.

## 2. When each mode is the right choice

### Mode A (v1) is the right choice when ANY of these is true:

- **Run count is bounded (<~hundreds concurrently in-flight)**.
  Dagster's daemon push-launch model handles this comfortably.
- **Per-task duration is short** (minutes, not hours). Run workers
  release quickly; orchestrator host capacity is not strained.
- **LSF queue is short** (PEND time typically < 5 min). The
  SUBMITTED/RUNNING distinction (whitepaper §5) does not pay off.
- **Native Dagster failure handling is sufficient**: run-retries,
  from-failure, UI re-execute cover the operational needs.
- **You want the standard Dagster UI** as the source of truth. Green
  light = done; no caveats for operators.
- **The team is new to the framework**. Mode A is structurally simpler
  and aligns with stock Dagster mental models, so onboarding is fast.
- **It's a PoC / prototype / one-off**. Don't pay v2's complexity tax
  for transient work.

### Mode B (v2) is the right choice when ANY of these is true:

- **Run count crosses ~thousands concurrently in-flight**. Above this,
  Dagster's daemon push-launch becomes the bottleneck (whitepaper §2.3;
  observed: 512 configured → only 60–80 actually running).
- **Per-task duration is long** (≥ 1 hr typical, overnight common).
  Run-worker-per-computation is wasteful at this duration: the worker
  is a coordination shell hogging resources next to the actual job.
- **LSF queue is long** (PEND time can be 30 min – several hours). The
  SUBMITTED vs RUNNING distinction becomes operationally critical:
  it's the only way to tell "need more LSF quota" from "need more
  workers" (whitepaper §4.3).
- **Incremental re-run pattern is dominant** (e.g. ~1% changes after a
  large run). The status DB's terminal-state lookup is faster than
  Dagster's `get_materialized_partitions` at scale, and the harvest /
  dispatch separation makes a "kick the 1% missing" loop cheap.
- **The "green light but still running" UI illusion is unacceptable**.
  This forces a self-built UI that reads the status DB and so removes
  Dagster UI from the operator critical path (whitepaper §3.3).
- **You can afford the implementation budget**: roughly 2× the
  framework complexity and ~8.5 days to build Phase 1 from scratch.

### Grey-zone heuristics

If you fall between, ask in this order:

1. **What's the run count × task duration product** (= total
   in-flight worker-hours)? If > 1k worker-hours, Mode B; if < 100
   worker-hours, Mode A. The middle is genuinely ambiguous — pick
   the simpler one (Mode A) and migrate later if it bites.
2. **How quickly does the team need to ship?** Mode A's existing
   `spec_dagster/` reference is ready to copy; Mode B needs Phase 1
   built first.
3. **How loud is the "UI is lying" complaint?** A single instance of
   "the UI said done but the job is still in LSF" is usually
   tolerable. Repeated, mission-critical confusion is not — escalate
   to Mode B.

## 3. Decision tree (mechanical; for weak-agent use)

Run through in order; first match wins.

```
1. Single-batch run count?
   <  100         → Mode A
   100 – 1000     → Mode A (Mode B is overkill)
   1000 – 10000   → CONTINUE to step 2
   > 10000        → Mode B (required)

2. Typical per-task duration?
   < 10 min       → Mode A
   10 min – 1 hr  → CONTINUE to step 3
   1 hr – 8 hr    → Mode B (strongly recommended)
   > 8 hr         → Mode B (required)

3. Typical LSF PEND time on the target queue?
   < 5 min        → Mode A
   5 – 30 min     → CONTINUE to step 4
   > 30 min       → Mode B (required; SUBMITTED/RUNNING distinction
                              pays for itself in operational triage)

4. Incremental re-run dominance?
   Re-run pattern is "occasional full" → Mode A
   Re-run pattern is "1% kicks, daily" → Mode B
   Mixed                              → Mode A first; revisit if
                                         dispatch sensor reconcile
                                         takes > 30 s per tick

5. UI requirement?
   "Dagster UI is fine"            → Mode A (you're already in Mode A
                                     by step 1; stay there)
   "Green-light-but-running is a
    real failure mode for us"      → Mode B (you must already be at
                                     Mode B by step 1/2; this just
                                     confirms it)
```

If steps 1–4 all said "continue / Mode A", **the answer is Mode A**.
If any step said "Mode B (required)" before reaching the end, the
answer is **Mode B**.

## 4. Mode-specific quick-start

### Starting in Mode A

1. Read `FIVE_LAYER_WHITEPAPER.md` (architecture).
2. Read `spec_dagster/ONBOARDING.md` (8-step SOP for flow owners).
3. `cp -r spec_dagster/flows/_template spec_dagster/flows/my_flow`,
   edit `spec.yaml` + `script.py`. Done.

### Starting in Mode B

> **Status (2026-06-05)**: Phase 1 not yet implemented. The plan in
> `PHASE_1_PLAN.md` describes the build sequence. Until that lands,
> start in Mode A and migrate when v2 is ready (see §6 below).

1. Read `EXECUTION_FABRIC_WHITEPAPER.md` (architecture; especially
   §3 three-domain model and §8 Phase 1 scope).
2. Read `PHASE_1_PLAN.md` (file-level implementation breakdown).
3. (When v2 lands) `cp -r execution_fabric/flows/_template execution_fabric/flows/my_flow`.

## 5. Hybrid considerations

Could a single flow have some assets in Mode A and others in Mode B?

**Phase 1 answer: not supported. Pick one mode per flow.** The
mechanics:

- Mode A's compute asset blocks until subprocess returns; Mode B's
  compute asset dispatches and returns immediately. Mixing inside one
  asset graph would mean the dispatch sensor (Mode B's reconciler)
  would need to ignore Mode A assets, and Dagster's lineage truth
  source would diverge per asset.
- A `spec.yaml`'s `dispatch:` field already exists (`local` for
  in-process subprocess; `lsf` for LSFRunLauncher in v1's pre-pivot
  design); the field could in principle be extended to
  `dispatch: fabric`. **This is intentionally not done in Phase 1** to
  keep the mental model clean.
- If a real use case demands it later (one flow with one heavy LSF
  cluster compute + many cheap generators), Phase 2 can revisit. Until
  then, the generator-vs-compute split inside one Mode-A flow already
  handles most "mixed weight" cases — generators are cheap to run
  in-process even at Mode B scale.

## 6. Migration paths

### A → B (you started in Mode A, hit a scaling wall)

The good news: **the spec layer is the same**.
- `spec.yaml` is identical (Mode B will introduce small additions,
  but the existing dimensions / assets / deps / partitioned_by /
  op_tags stay).
- `script.py` is largely identical: generator functions return
  `dict[abs_path, content]` (unchanged); compute functions return
  `argv: list[str]` (unchanged). The framework's wrapping changes,
  not your code.
- `_vendor/` artifacts (mock tools, vendored render/config) are
  unchanged.

What changes:
- Asset builder's compute path (framework code; not yours).
- Sensor model (framework code).
- A new fabric layer (framework code: status DB + LSF run client + harvest).
- The instance config (`dagster.yaml`).

**Estimated owner-side migration effort per flow: < 1 day** once Phase
1 framework exists. The estimate assumes no application-side rework.
Most of the ~8.5d in `PHASE_1_PLAN.md` is the **framework**, not the
**flow**.

### B → A (very rare; you over-engineered)

Discouraged. Mode B's split-truth model is harder to live with at
small scale than Mode A's straightforward model, but going back means
giving up the harvest sensor and bringing computation back into the
asset body. Possible mechanically — same spec / script — but consider
whether your scale really shrank.

## 7. Anti-patterns (don't do these)

| Anti-pattern | Why it's wrong | Right thing |
|---|---|---|
| Mode B at PoC scale "for futureproofing" | You pay 2× complexity today for a future you may never reach | Start Mode A; migrate only when the decision tree §3 says so |
| Mode A at 10k+ runs because "we already have the code" | Daemon push-launch will throttle to 60–80 concurrent regardless of your `max_concurrent_runs` setting | Migrate to Mode B; the scaling wall is structural, not configurable |
| Mode B with Dagster UI as the operator dashboard | UI shows green when dispatch finished, not when computation finished. Operators will misread state | Mode B requires self-built UI reading status DB (whitepaper §3.3) |
| Mixing Modes within one asset graph | Lineage truth source becomes per-asset; harvest sensor and dispatch sensor diverge in scope | Pick one mode per flow |
| Mode B without writing PHASE_1_LIMITATIONS.md acceptance | Phase 1 explicitly accepts: no orphan recovery, single-writer SQLite, no bjobs sync. Operating without operator awareness of these = silent failures | Document and operationalize the limits before going live |
| Using both modes' whitepaper as if they're sequential ("v2 is the latest version") | They're parallel products with different scaling envelopes | Treat them as siblings, not generations |

## 8. Document map

```
personalities/flow-cartographer/
├── README.md                          # personality INDEX (start here)
├── ARCHITECTURE_CHOICE.md             # THIS FILE — pick a mode
├── FIVE_LAYER_WHITEPAPER.md           # Mode A spec
├── EXECUTION_FABRIC_WHITEPAPER.md     # Mode B spec
├── D2_IMPLEMENTATION_PLAN.md          # Mode A: liberate-char on framework (executed in PR #18)
├── PHASE_1_PLAN.md                    # Mode B: Phase 1 implementation plan (pending)
└── examples/liberate-char/            # hand-rolled reference (predates both Modes;
                                       # the example the framework was extracted from)

spec_dagster/                          # Mode A framework (D1+D2 verified)
└── flows/liberate_char/               # Mode A reference application

execution_fabric/                      # Mode B framework (Phase 1 — not yet built)
└── flows/liberate_char/               # Mode B reference application (planned)
```

## 9. Status (2026-06-05)

| Mode | Status | Evidence |
|---|---|---|
| A | **Production-ready** (D1+D2 verified) | `spec_dagster/scripts/run_demo.py` PASS; `scripts/equivalence.py` PASS all C1–C5; pytest 40/40; PR #18 |
| B Phase 1 | **Planned, not built** | `PHASE_1_PLAN.md` (§10 Q4 probe answered 2026-06-05; technical unknowns cleared; awaiting decision on §1 organizational questions before build) |
| B Phase 2 | **Future** | Whitepaper §9 (Kafka + PostgreSQL + full fault contracts + worker pool) |

## 10. When this document is wrong

If your situation makes a Mode-A use case feel like Mode B (or vice
versa), trust the use case, not this document. The decision tree §3 is
a guideline calibrated to the EDA characterization workload that
motivated both whitepapers. New workloads may sit in unexpected
quadrants. Document the deviation in your flow's own README so future
maintainers understand the rationale.
