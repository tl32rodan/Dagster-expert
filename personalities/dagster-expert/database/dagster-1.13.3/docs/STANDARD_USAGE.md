# Air-gap Dagster 1.13.3 — Standard Usage (the ONE prescribed path)

**Tested against Dagster 1.13.3.** This is the single golden path for this
deployment: architecture → daemon → triggers → operation interface. **If your
question is answered here, do not search further — no SMAK semantic search, no
training memory.** If it is *not* answered here, say so and stop (or file an
`_inbox/` case study); do not improvise.

> **繁中導讀**：這份是唯一的標準用法文件。架構、daemon、觸發、操作介面都以這裡為準。
> agent 只走這條路徑；查不到就明說並停手，不要亂猜。每段先給英文規範，再用
> `> 繁中：` 補充說明給人看。

**The model in one line — UI = observe, CLI = execute, sensors = trigger.**
Avoid wide backfills; prefer sensor-driven incremental runs and targeted CLI
materializes.

> 繁中：UI 只拿來看狀況；執行一律走 CLI；觸發靠 sensor 增量驅動。盡量不要大範圍 backfill。

---

## 0. DO / DON'T at a glance

| DO (golden path) | DON'T (deprecated / unsuitable / out-of-scope) |
|---|---|
| Model deps **asset-centric**: `@asset` + `deps=[AssetDep(key, partition_mapping=…)]`, implicit `__ASSET_JOB`. | ❌ `@job` / `define_job` + partition sets to manage partitioned pipelines — **outdated**; asset-centric replaces it. |
| Define relationships in **external files** (YAML/JSON) + pure-data `spec/`+`rules/`; a `factory` builds assets. Change relationships = edit the file/rules. | ❌ Hand-wire each `@asset`'s deps inline so a relationship change means editing asset code. |
| Many-to-many via a **pre-computed, memoized built-in `StaticPartitionMapping`**. | ❌ **Subclass `PartitionMapping`** — emits `DeprecationWarning: Non-built-in PartitionMappings … will not work with asset reconciliation`. |
| One **module-level `partitions_def` singleton** per shape; group with `group_name`. | ❌ Construct a fresh `partitions_def` per asset — mapping silently degrades to "all partitions". |
| Execute via **CLI** (`dagster asset materialize --select --partition`) + **sensor-driven** incremental. | ❌ Use the UI **"Materialize all"** button as an execution path. |
| Observe in the **UI**; serve analysts a `dagster-webserver --read-only` instance. | ❌ Routine wide-range `dagster job backfill` for thousands of partitions. |
| Control concurrency with `QueuedRunCoordinator` `max_concurrent_runs` + `tag_concurrency_limits`. | ❌ **Subclass `RunCoordinator`** — not recommended/supported here in 1.13.3. |
| Run cluster work from **inside the asset body** via `PipesSubprocessClient` (e.g. `bsub`) + in-body queue throttle. | ❌ **Subclass `RunLauncher`** to push runs to a cluster — "much more work / out of scope". |
| Instance: `DefaultRunLauncher`, Postgres beyond solo dev, `telemetry.enabled: false`, `run_monitoring` on. | ❌ Reach for a custom multi-thread launcher to "parallelize" a backfill — **wrong layer** (see §9c). |
| Keep N>2 partition dimensions as a **composite key** (2-dim limit). | ❌ 3-dim `MultiPartitionsDefinition`; `dagster._core.*` imports; `dg`/`uv`/`pipx`/Poetry/k8s/Helm/public PyPI/Docker/telemetry/Dagster+. |

> 繁中：上表就是「該做 / 不該做」總表。左欄照做；右欄是已過時、不合用、或超出 air-gap 範圍，
> 一律不要做。下面各節是每一條的展開與理由。

---

## 1. The golden path

For any "how do I run X" question, pick the **lowest** row that satisfies the need:

1. **Sensor-driven incremental** — preferred. The daemon fires a sensor that
   requests exactly the partitions whose upstreams changed.
2. **Targeted CLI materialize** — `dagster asset materialize --select <key> --partition <p>`.
3. **Schedule** — cron-triggered, for periodic refresh.
4. **Manual UI button** — discouraged; observation only.

> 繁中：能用 sensor 就用 sensor；要手動就用 CLL 點名 partition；週期性用 schedule；
> UI 按鈕當作最後手段（其實是不該用來執行）。

---

## 2. Architecture — the five processes

| Process | Binary | Role | One per instance? |
|---|---|---|---|
| Webserver | `dagster-webserver` | UI + GraphQL (observe; launch only if not `--read-only`) | many ok |
| Daemon | `dagster-daemon run` | schedules, sensors, run queue, run monitoring | **exactly one** |
| Code server(s) | `dagster code-server start` | load your `Definitions` over gRPC | one per code location |
| Run worker | spawned by the run launcher | executes one run's steps | one per run |
| CLI | `dagster …` | execute / inspect / backfill | n/a |

Dev shortcut `dagster dev -w workspace.yaml` runs the webserver **and** daemon
in one process — fine for a single developer, not for production. (See
`skills/cli-cheatsheet/SKILL.md` "the five binaries".)

> 繁中：五個角色。生產上 webserver 與 daemon 分開跑、daemon 全instance只能有一個；
> 開發時 `dagster dev` 一次把兩者跑起來。code server 用 gRPC 載入你的 `Definitions`。

---

## 3. Recommended architecture for cross-asset partition dependencies

This is the answer to "I have many-to-many cross-asset partition deps and need
custom mapping — how do I group and execute?" **Do not subclass `PartitionMapping`,
and do not use jobs.** Use the external-file-driven, asset-centric, layered
pattern that `demo/scale-lib/` already implements.

### 3.1 Five layers (relationships are DATA, not code)

| Layer | Where | Touches Dagster? | Role |
|---|---|---|---|
| 0 Config | `demo/scale-lib/config/*.yaml,*.json` | no | **External relationship files** — edit these to change relationships |
| 1 Spec | `pipelines/spec/*.py` | no | Pure data: hierarchy, `PartitionRule` protocol + impls |
| 2 Rules + Registry | `pipelines/rules/*.py`, `registry.py` | no | `DepRule.emit_edges()` → `DepEdge`; registry merges same-target edges; one source of truth |
| 3 Translator | `pipelines/translator.py` | yes | `PartitionRule` → **built-in** `StaticPartitionMapping` / `IdentityPartitionMapping` / `SpecificPartitionsPartitionMapping` |
| 4 Factory | `pipelines/factory.py` | yes | Loop rules → `AssetDep(partition_mapping=…)` → `@asset(key_prefix, group_name, partitions_def=<singleton>)` |

A `test_layer_imports.py` enforces that layers 0–2 import nothing from Dagster,
so the relationship model stays portable and testable.

> 繁中：關係都是資料。改關係＝改 config 檔或 rules，不改資產碼。spec/rules/registry 完全
> 不 import Dagster（有測試強制），只有 translator/factory 碰 Dagster。這就是「外部檔定義關係」。

### 3.2 Many-to-many — the only correct recipe in 1.13.3

Reconciliation/auto-materialize only understands the **built-in** mappings.
Express any many-to-many shape as a **pre-computed `StaticPartitionMapping`**,
built by enumeration at definition load and **memoized as a module-level
singleton**. Verbatim from `learn/20-multi-library-grain/pipelines/edges.py:59-91`:

```python
from dagster import StaticPartitionMapping

def _parent_mirror_static_mapping() -> StaticPartitionMapping:
    # For each downstream branch b: wanted_upstream = {b} ∪ {parent_of(b)}.
    # Then invert to {upstream: [downstream_keys_that_need_it]}.
    upstream_to_downstream: dict[str, set[str]] = {}
    for downstream in all_branches():
        wanted = {downstream}
        p = parent_of(downstream)
        if p is not None:
            wanted.add(p)
        for up in wanted:
            upstream_to_downstream.setdefault(up, set()).add(downstream)
    return StaticPartitionMapping(
        downstream_partition_keys_by_upstream_partition_key={
            up: sorted(downs) for up, downs in upstream_to_downstream.items()
        },
    )

_PARENT_MIRROR = _parent_mirror_static_mapping()   # memoize: build once, reuse everywhere
```

**Reconciliation-safe built-in mappings (the only ones permitted):**
`AllPartitionMapping`, `IdentityPartitionMapping`, `LastPartitionMapping`,
`SpecificPartitionsPartitionMapping`, `StaticPartitionMapping`,
`TimeWindowPartitionMapping`, `MultiToSingleDimensionPartitionMapping`,
`MultiPartitionMapping`. (Source: `memory/understanding/dagster-1.13.3-gotchas.md:51-81`.)

Attach with `deps=` (not `ins=`, which would force IO loading —
gotcha #3):

```python
from dagster import AssetDep, AssetKey
deps = [AssetDep(asset=AssetKey([lib, "step4"]), partition_mapping=_PARENT_MIRROR)]
```

> 繁中：多對多唯一正解＝**預先列舉、算好一張 `StaticPartitionMapping`，存成模組級單例**。
> 自寫 `PartitionMapping` 子類會跳棄用警告且不與 reconciliation 相容。允許的內建 mapping
> 就上面那 8 個。掛載用 `deps=` 不要用 `ins=`。

### 3.3 Grouping & execution (no jobs)

- **Grouping for the UI**: `group_name` on each asset (`factory.py:58`). There is
  no `@job` anywhere (`definitions.py:19`); Dagster builds the implicit `__ASSET_JOB`.
- **Dependency grouping** (which upstream partitions feed which downstream) is the
  pre-computed `StaticPartitionMapping` — *not* a job boundary.
- **Execution**: select by asset key + partition key via CLI or a sensor. Before
  `step5[lvf]` runs, both `step4[lvf]` and `step4[corner]` must be ready — Dagster
  derives that from the mapping.

> 繁中：分組用 `group_name`（UI 導覽）；依賴分組是那張 mapping，不是 job。執行靠
> 「資產鍵＋partition 鍵」用 CLI 或 sensor 點名，不靠 job/partition-set。

### 3.4 Adopting this in your repo

Copy the 5-layer skeleton from `demo/scale-lib/pipelines/`. Keep your relationship
truth in `config/` + `rules/`; only `translator.py` + `factory.py` import Dagster.

---

## 4. Daemon

The daemon (`dagster-daemon run`, exactly one) drives everything time- or
event-based: **schedule ticks, sensor ticks, the run queue, and run monitoring.**

- Dev: `dagster dev` includes the daemon. Production: run `dagster-webserver` and
  `dagster-daemon run` as **separate** services (e.g. under systemd).
- Health probe: `dagster-daemon liveness-check` (exit 0 if healthy).
- **Config is read at process start — no hot reload.** Restart the daemon +
  webserver after any `dagster.yaml` change.
- If schedules/sensors don't fire or the queue piles up, first check the daemon is
  alive and ticking.

> 繁中：daemon 全instance只有一個，負責 schedule/sensor/queue/run-monitoring。生產上和
> webserver 分開跑。改 `dagster.yaml` 後一定要重啟（不會熱載入）。schedule/sensor 不動
> 先查 daemon 活著沒。

---

## 5. Trigger mechanisms — prescribed hierarchy

| I want to… | Use | Command / construct | Notes |
|---|---|---|---|
| Rebuild only what changed | **Sensor** (preferred) | `@sensor` / `@asset_sensor` + `RunRequest` | daemon-driven; dedup via `run_key`; throttle via `minimum_interval_seconds` |
| Run one/few specific partitions | **CLI** | `dagster asset materialize -w workspace.yaml --select <key> --partition <p>` | targeted; deterministic |
| Periodic refresh | **Schedule** | `ScheduleDefinition` + cron | daemon must run; mind `execution_timezone` |
| Hands-off per-partition rebuild | **Auto-materialize** | `AutoMaterializePolicy.eager()` / `.lazy()` | daemon-evaluated; see lessons 16/19 (verify exact API on your install) |
| Click to run in the UI | **discouraged** | "Materialize" / "Materialize all" | observe only; see §9b for why |

Avoid wide `dagster job backfill` as routine; prefer sensors that request only the
stale partitions. (Schedules → lesson 14; sensors → lesson 15.)

> 繁中：優先順序＝sensor＞CLI 點名＞schedule＞（盡量別用）UI 按鈕。大範圍 backfill 不要當
> 日常手段，改用 sensor 只要求變髒的 partition。

---

## 6. Operation interface — UI vs CLI vs GraphQL

| Surface | Use it for | Don't use it for |
|---|---|---|
| **UI** (`dagster-webserver`) | lineage, partition heatmap, run timeline, logs, status | executing work (use CLI/sensors) |
| **CLI** (`dagster …`) | execute, materialize, inspect, backfill | n/a |
| **GraphQL** (`dagster-graphql`) | automation / scripting against a running webserver | ad-hoc human work |

Common UI action → CLI equivalent → what it does:

| UI action | CLI | What it does |
|---|---|---|
| Materialize one partition | `dagster asset materialize -w workspace.yaml --select <key> --partition <p>` | launches one run for that partition |
| Materialize all | (avoid) `dagster job backfill -w workspace.yaml -j __ASSET_JOB --partition-set <set>` | enqueues one run **per partition** |
| Inspect assets | `dagster asset list -w workspace.yaml --select "key:<k>+"` | lists assets + downstream |
| Read-only analyst UI | `dagster-webserver -w workspace.yaml --read-only -p 3001` | UI works, no launches/wipes |

(Full surface: `skills/cli-cheatsheet/SKILL.md`.)

> 繁中：UI 看、CLI 做、GraphQL 自動化。給 analyst 的 UI 開 `--read-only`，他們只能看不能動。
> 上表把 UI 動作對到 CLI 指令並說明實際行為。

---

## 7. Knobs & mainstream defaults

### 7.1 Instance — `$DAGSTER_HOME/dagster.yaml`

| Knob | What it does | Standard (air-gap) |
|---|---|---|
| `telemetry.enabled` | outbound usage stats | **`false`** (non-negotiable) |
| `storage` | SQLite vs Postgres | **Postgres** beyond solo dev (SQLite is single-writer → "database is locked") |
| `run_launcher` | where/how a run executes | **DefaultRunLauncher** (subprocess); Docker optional; **K8s refused** |
| `run_coordinator` + `max_concurrent_runs` | how many runs at once | **QueuedRunCoordinator**, cap tuned to host |
| `tag_concurrency_limits` | cap a tagged family | throttle heavy families via `dagster/concurrency_key` |
| `run_monitoring` | auto-fail orphan STARTED runs | **enabled** with timeouts |
| `compute_logs` / `local_artifact_storage` | stdout/stderr + default IO writes | local FS single-host; shared NFS / MinIO multi-host |

```yaml
# $DAGSTER_HOME/dagster.yaml — production essentials (see skills/dagster-yaml-reference)
run_launcher:
  module: dagster._core.launcher
  class: DefaultRunLauncher
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10
    tag_concurrency_limits:
      - { key: dagster/concurrency_key, value: heavy_eda, limit: 2 }
run_monitoring: { enabled: true, start_timeout_seconds: 300, max_runtime_seconds: 86400 }
telemetry: { enabled: false }
```

### 7.2 Workspace — `workspace.yaml`

`python_module` (dev, in-process) vs `grpc_server` / `dagster code-server start`
(production); single vs split-per-library code locations. (See
`skills/workspace-yaml-reference/SKILL.md`.)

### 7.3 Asset / definition

- `partitions_def` **module-level singleton** per shape.
- PartitionMapping: built-ins only (§3.2); **no custom subclass**.
- `MultiPartitionsDefinition` **2-dim hard limit** → composite key (see `partitions.md`).
- `RetryPolicy(max_retries, delay, backoff, jitter)` on `@asset` (see `failures-retries.md`).
- `AutoMaterializePolicy.eager()` (critical path) vs `.lazy()` (reports) — lessons 16/19.

> 繁中：knob 分三層：instance（dagster.yaml）、workspace、資產定義。標準值如表。`dagster.yaml`
> 改完要重啟。partitions_def 要單例、mapping 只用內建、MultiPartitions 上限 2 維。

---

## 8. Run launcher & run coordinator design

**Configure built-ins; delegate cluster work via Pipes; do NOT subclass.**

- **Run launcher.** Standard `DefaultRunLauncher` (subprocess). For LSF/cluster,
  the **asset body** calls `PipesSubprocessClient.run([... "bsub" ...])` — see
  `learn/13-lsf-integration/pipelines/asset.py:48-83` and
  `skills/lsf-executor/SKILL.md`. Subclassing `RunLauncher` is "possible but adds
  complexity / out of scope".
- **Run coordinator (queuing).** Use `QueuedRunCoordinator` and tune two knobs:
  `max_concurrent_runs` (instance cap) and `tag_concurrency_limits` (cap a family
  keyed on `dagster/concurrency_key`). Do **not** subclass `RunCoordinator`.

> 繁中：launcher 用內建 Default，叢集工作從「資產 body 內」用 Pipes 丟 bsub；不要自寫 launcher。
> 排隊用 QueuedRunCoordinator 調 `max_concurrent_runs` 與 `tag_concurrency_limits`；不要自寫 coordinator。

---

## 9. Design-vs-usage gaps (the traps)

### 9a. Partition consistency

A `partitions_def` must be **one module-level singleton object** shared by every
asset of the same shape. Two different objects (even with identical keys) make
cross-asset mapping silently degrade to "all partitions".
**Fix:** define `branch_partitions = StaticPartitionsDefinition(...)` once at module
level and import it everywhere. (See `learn/20-multi-library-grain/README.md:173`.)

### 9b. "Materialize all" is N runs, not one

Clicking **Materialize all** on an N-partition asset enqueues **N separate
RunRequests** (one per partition). They drain subject to
`QueuedRunCoordinator.max_concurrent_runs`. A custom multi-thread launcher does
**not** change this — it changes how a *single* run executes, not how many runs are
submitted or how fast the queue drains.
**Fix:** for controlled parallelism use targeted CLI / sensors per §1; tune
`max_concurrent_runs` to set the parallelism ceiling.

### 9c. The four levels of concurrency (why "Materialize all" looked sequential)

| Level | Knob | Controls |
|---|---|---|
| 1 Instance run concurrency | `max_concurrent_runs` | how many **runs** at once |
| 2 Family concurrency | `tag_concurrency_limits` | cap a tagged **group** of runs |
| 3 Within-run step concurrency | job/asset executor (`multiprocess_executor`) | parallel **steps inside one run** — the corpus uses Pipes fan-out instead; verify config via `python -m pydoc dagster.multiprocess_executor` |
| 4 Execution backend | `run_launcher` | **where/how** a run executes — NOT how many, NOT how submitted |

**Key insight:** "Materialize all" parallelism is governed by **level 1**, never by
level 4. A multi-thread launcher cannot parallelize a backfill. (Op-level
concurrency "pools" are a newer-Dagster feature — do **not** assume they exist in
1.13.3; use `tag_concurrency_limits`.)

> 繁中：三個坑——(a) partitions_def 必須單例；(b)「Materialize all」是 N 個 run 不是一個，受
> `max_concurrent_runs` 節流；(c) 併發有四層，按鈕的並行只由第 1 層決定，跟 launcher（第 4 層）無關。
> 自寫多執行緒 launcher 不會讓 backfill 變並行。

---

## 10. Cross-references (read for depth; don't duplicate)

- Partitions / 2-dim limit: `database/dagster-1.13.3/docs/partitions.md`
- Run states / retry / re-execute: `database/dagster-1.13.3/docs/failures-retries.md`
- Cross-location deps: `database/dagster-1.13.3/docs/cross-location.md`
- API gotchas (incl. mapping deprecation): `memory/understanding/dagster-1.13.3-gotchas.md`
- Partition mapping teaching: `learn/17-incremental-cross-partition/`
- Canonical many-to-many: `learn/20-multi-library-grain/pipelines/edges.py`
- Full reference implementation: `demo/scale-lib/`
- Cluster execution via Pipes: `skills/lsf-executor/SKILL.md`, `learn/13-lsf-integration/`
- CLI / dagster.yaml / workspace.yaml: `skills/cli-cheatsheet/`, `skills/dagster-yaml-reference/`, `skills/workspace-yaml-reference/`
- Schedules / sensors: lessons 14 / 15

---

## 11. Air-gap readiness checklist

- [ ] `echo $DAGSTER_HOME` non-empty; `dagster.yaml` lives **inside** `$DAGSTER_HOME` (gotcha #10)
- [ ] `dagster --version` → `1.13.3`; `which dagster` inside the venv
- [ ] `telemetry.enabled: false`; no `dg`/`uv`/k8s/public PyPI/Dagster+
- [ ] Postgres if daemon + webserver run concurrently
- [ ] exactly one `dagster-daemon run`; `run_monitoring` enabled
- [ ] partitions defined as module-level singletons; mappings are built-ins only
- [ ] analyst UI served with `--read-only`

> 繁中：上線前逐項打勾。DAGSTER_HOME、版本、telemetry、Postgres、單一 daemon、partition 單例、
> 唯讀 UI。
