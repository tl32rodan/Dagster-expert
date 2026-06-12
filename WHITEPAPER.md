# Execution Fabric — Architecture White Paper

> **文件目的**：說明 EDA characterization 編排系統的「為什麼」與「怎麼做」。讀者:接手實作、修改、或將既有 script 移植進本框架的工程師 / AI agent。
>
> **環境前提**:air-gapped 工作站、LSF job scheduler、NFS 共享儲存、可用 Kafka(無 RabbitMQ)、PostgreSQL 可部署於專用主機、tcsh/csh shell。Dagster 版本鎖定在 **1.13.7**。

---

## §0 TL;DR

- **核心分層**:把「**資料身分**」(lineage / data version / 增量判斷) 與「**執行過程**」(dispatch / 排隊 / 實際計算) 徹底分開。Dagster 只負責前者;自建的 Execution Fabric 負責後者。
- **執行模型**:**非阻塞 LSF client**。Dagster asset body 投遞 `bsub` 後立即返回;真正的計算在 LSF 節點上獨立進行,完成後 fabric_worker 自行計算 data_version 並寫進 status DB。Dagster sensor 從 status DB 拉狀態,以 runless asset event 回填 lineage。
- **設計鐵則**:
  1. asset body **絕不阻塞**(沒有 `-K`、沒有 wait)
  2. status DB 是執行真相的**唯一**權威來源
  3. **harvest sensor 是唯一**把「執行真相」轉成「身分真相」的縫合點;優先用 TDD 釘死
- **兩階段**:Phase 1 = SQLite + file lock,單機驗證 happy path;Phase 2 = PostgreSQL + Kafka,完整故障修復、10k+ 規模。
- **讀者地圖**:看架構 → §2;看實作 → §3;移植既有 script → §5;中斷/重跑 → §6;接收交棒(critical correctness) → §7。

---

## §1 背景與問題定義

### 1.1 工作負載特性

- 任務單位:cell-level characterization (每份計算數十分鐘至小時級)。
- 規模:單批可達 **10k+** 份計算;後續迭代常常只有約 **1%** 需要重跑。
- 痛點核心:**避免重複計算**。當少量上游變更時,只重算「受影響的下游」。這是整個系統存在的理由。
- LSF 排隊現實:`bsub` 到真正開始執行 (PEND → RUN) 通常數分鐘,塞車時可達數小時。此數字對狀態設計有決定性影響(見 §3 M4)。

### 1.2 為什麼是 Dagster

「1% 增量重跑」需要一個能表達依賴、並依「上游 data version 是否改變」判斷下游是否需重算的系統。Dagster 的 asset 模型 + `DataVersion` 機制天生勝任。**這是選用 Dagster 的唯一理由**——不是 queue、不是執行能力,而是 data-aware lineage。

### 1.3 為什麼不用 Dagster 原生的 run launcher

把「一個 LSF job = 一個 Dagster run」這個 push 模型在本工作負載下不可擴展:

- 每一份正在跑的計算,都需要一個活著的 run worker process 對應。10k 份長計算 = 同時 10k run worker。
- daemon 是「逐個 push `launch_run`」的模型;在「同時數千個長 run」時啟動吞吐成為序列瓶頸 (實測:設定 512 並行,實際只跑起 60–80 個)。

**結論**:Dagster 退化為「決定算什麼 + 收割結果」;執行完全在它看不見的執行層。

---

## §2 三領域框架(概念上層)

整套系統分三個領域,各有單一職責,**透過明確定義的「跨界線」資料流互動**。

### 2.1 領域一:資料身分(Dagster)

**鐵律**:只回答「該算什麼」與「算到什麼版本」;絕不回答「現在算到哪、worker 活著嗎」。

| 模組 | 職責 | 關鍵約束 |
|---|---|---|
| Asset graph | 用 asset 依賴表達 lineage;`DataVersion` 判斷哪些下游受影響需重算 | data_version 是「避免重複計算」的唯一依據 |
| Dispatch sensor | 讀 `desired − observed`,把缺的 partition 投遞到 Execution Fabric(寫一筆 PENDING 到 status DB + emit RunRequest) | **絕不在 sensor 內阻塞等計算** |
| Harvest sensor | 從 status DB 讀到 SUCCESS,用 `report_runless_asset_event` 回填 `AssetMaterialization`(帶真實 data_version) | 唯一縫合點;**必須冪等** |

**Dagster 元件配置**:`DefaultRunLauncher`、`QueuedRunCoordinator`(`max_concurrent_runs` 設小)、PostgreSQL run/event store(prod)或 SQLite(local-sim)。**只啟動 daemon,不啟動 webserver**(見 §2.3 為什麼)。

### 2.2 領域二:執行織體(Execution Fabric)

**鐵律**:Dagster 在此完全失明。此層透過兩個介面與外界接觸——status DB 的「投遞入口」與「結果出口」。

| 模組 | Phase 1 | Phase 2 |
|---|---|---|
| Task 投遞 | dispatch sensor 直接寫 PENDING 到 status DB | + Kafka priority queue 緩衝 |
| Status DB | SQLite + file lock(`fcntl.flock`) | PostgreSQL(高並發、跨主機) |
| LSF client | 非阻塞 `bsub`(無 `-K`),寫 lsf/job_id | 同 + 連線/重試強化 |
| Fabric worker | 在 LSF 節點上跑 script,**自行計算 data_version**,寫 status DB SUCCESS | 同 + heartbeat |
| LSF 狀態同步 | (略)Phase 1 conflates SUBMITTED + RUNNING | bjobs 定期同步 SUBMITTED ↔ RUNNING |
| Reaper | (無) | bjobs-driven 孤兒回收 |

### 2.3 領域三:Control + UI

**鐵律**:只讀聚合 + 觸發入口;**永不持有自己的狀態真相**。

| 模組 | 職責 | 讀寫關係 |
|---|---|---|
| Control logic | 決定重跑哪些、調整 worker pool 大小 | 對 status DB 只讀;透過 event/resource control 下指令 |
| UI | 從 Dagster 讀 lineage、從 status DB 讀執行進度 | 雙邊只讀 |

**為什麼不用 Dagster 原生 UI**:Dispatch run 在 Dagster UI 上會很快變綠燈(因為它只負責投遞),但真正的計算還在執行層跑。這會造成「綠燈但其實還在 run」的認知陷阱。自建 UI 直接讀 status DB,呈現的是執行真相。

### 2.4 跨界線資料流

```
Control ──(event control)──> Dagster asset graph
                                   │ lineage 判斷哪些需算(上游 version 沒變則不投遞)
                                   ▼
                            Dispatch sensor(短命 run)
                                   │ 投遞 request(廉價,一次可投海量)
                                   ▼  ← 跨界線:向下穿過,Dagster 放手
                            Execution Fabric(Dagster 失明)
                              Status DB → LSF Client → bsub → LSF
                                                            ↓
                                                      Fabric worker
                                                            │ 算完,原子寫入 SUCCESS + data_version
                                                            ▼
                            Status DB(執行真相)
                                   │ harvest sensor 讀取
                                   ▲  ← 跨界線:向上穿回,Dagster 重新接管身分
                            Harvest sensor → report_runless_asset_event
                                   │
                                   ▼
                            Dagster lineage 推進(身分真相更新)
```

**最關鍵的兩條線**:
1. **投遞線(向下)**:request 穿過邊界進入 Fabric 時,Dagster 放手——不追蹤它的執行。
2. **收割線(向上)**:結果穿回邊界時,Dagster 才重新接管它的身分。harvest sensor 是「執行真相 → 身分真相」**唯一交棒點**,整個系統的正確性壓在它的冪等性上。

### 2.5 Source of truth 主從關係

| 真相類型 | 權威來源 | 衍生方 |
|---|---|---|
| 執行狀態(跑了沒、結果、data_version) | **Status DB**(fabric_worker 寫) | — |
| 身分 / lineage(算到什麼版本、依賴推進) | **Dagster materialization** | 衍生自 harvest sensor 從 status DB 讀到的結果 |
| 控制決策 | Control logic(不落地狀態,只下指令) | — |

---

## §3 M1–M5 機制(層級實作底層)

三領域是概念分層;M1–M5 是程式碼層級的職責切分,對應 `execution_fabric/framework/` 的模組結構。

### 3.1 M1 — Spec 層

**對象**:flow owner。**檔案**:`flows/<name>/spec.yaml`。**驗證器**:`framework/spec/schema.py`(Pydantic)+ `framework/spec/loader.py`。

```yaml
version: 1
flow_name: <name>
dimensions:
  <dim>: { type: static, values: [...] }
assets:
  - { name: <root>, kind: entry, partitioned_by: [] }
  - name: <generator>
    kind: generator
    script: flows.<name>.script:<fn>
    partitioned_by: [<dim>]
    depends_on: [{ asset: <root>, mapping: all }]
  - name: <leaf>
    kind: compute
    script: flows.<name>.script:<fn>
    partitioned_by: [<dim1>, <dim2>]
    depends_on:
      - { asset: <generator>, mapping: { <dim1>: identity } }
    lsf: { queue: normal, cores: 4, mem_mb: 4096, walltime: "24:00" }
```

**Schema 簡化(vs. v1)**:`Dispatch` 與 `Trigger` 兩個欄位被移除。只有一條路徑——`compute` 走 Fabric。

### 3.2 M2 — Definitions 產生器

**檔案**:`framework/generator.py`、`framework/assets/builder.py`、`framework/assets/{partition,mapping}_builder.py`。

`build_definitions(flows_dir)` 掃描 `flows/*/spec.yaml`,為每個 flow:
1. 構建 `MultiPartitionsDefinition`(從 `dimensions`)。
2. 構建 `MultiToSingleDimensionPartitionMapping`(從 `depends_on.mapping`)。
3. 為每個 asset 構建 `@asset`:
   - `entry` / `generator`:in-process 計算,直接 emit `MaterializeResult` 含 content_hash data_version。
   - `compute`:body 計算 idempotency_key → 寫 PENDING → 呼叫 `lsf_run_client.dispatch(...)` → **return None**。harvest sensor 後續才補上真實 materialization。
4. 構建 **dispatch sensor** 與 **harvest sensor**(每 flow 各一個)。
5. 組裝 `dg.Definitions`。

### 3.3 M3 — 雙 sensor 層

**這層取代了 v1 的「reconcile sensor + cascade sensor」**。現在拆成投遞 + 收割兩個職責。

#### Dispatch sensor (`framework/sensor/dispatch.py`)

```
每 tick:
  for each compute asset:
    desired   = partition keys (from PartitionsDefinition)
    observed  = status_db.list_keys_with_state_in(asset, ['SUCCESS'])
    missing   = desired − observed
    for each missing partition:
      idempotency_key = sha256(asset, partition, sorted(upstream data_versions))
      status_db.upsert_pending(idempotency_key, asset, partition, upstream_dvs)
      emit RunRequest(partition_key=partition, run_key=idempotency_key)
```

**為什麼 `observed` 來自 status DB 而非 Dagster materializations**:dispatch asset body return None 後,Dagster 會自動 emit 一個 placeholder materialization(帶自動算的 data_version)。如果 dispatch sensor 看 Dagster materializations,會看到 placeholder 並誤判「已算完」,不再 redispatch。**Status DB 才是執行真相**。(這就是 §2.3 棄用 Dagster UI 的原因——它顯示的是 dispatch 真相而非執行真相。)

#### Harvest sensor (`framework/sensor/harvest.py`)— 最脆弱的模組

```python
@sensor(job=_noop_job, default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=30)
def harvest_sensor(context):
    cursor = json.loads(context.cursor or '{"last_id": 0}')
    rows = status_db.list_unharvested_terminals(after_id=cursor["last_id"], limit=200)
    processed = []
    for row in rows:
        try:
            context.instance.report_runless_asset_event(
                AssetMaterialization(
                    asset_key=AssetKey(row.asset_name),
                    partition=row.partition_key,
                    tags={"dagster/data_version": row.data_version},
                    metadata={"lsf_job_id": row.lsf_job_id}
                        if row.lsf_job_id else None,
                )
            )
            processed.append(row.id)
        except Exception as e:
            context.log.error(f"harvest failed for row {row.id}: {e}")
            break   # 不推進 cursor → 下次重試
    if processed:
        status_db.mark_harvested(processed)        # 先確認回填,再標記
        new_last = max(processed)
        return SensorResult(cursor=json.dumps({"last_id": new_last}),
                            skip_reason=SkipReason(f"harvested {len(processed)} rows"))
    return SkipReason("no progress this tick")
```

**正確性鐵則**:
1. **先回填 Dagster 再推進 cursor**;失敗則 cursor 停留 → 下次重試。
2. `report_runless_asset_event` **天生冪等**(latest-wins,重複 append 無害)。
3. `job=` 參數是 1.13.7 `@sensor` 的 schema 要求;我們從不真的 enqueue 該 job(`SkipReason` 並 emit side effect)。

詳細測試要求見 §7.3。

### 3.4 M4 — Execution Fabric 層

**檔案**:`framework/fabric/{status_db.py, file_lock.py, lsf_run_client.py, schema.sql}`、`framework/config/dagster.fabric.yaml`。

#### Status DB(SQLite Phase 1 → PostgreSQL Phase 2)

```sql
-- framework/fabric/schema.sql
CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT    NOT NULL UNIQUE,
    asset_name      TEXT    NOT NULL,
    partition_key   TEXT,                       -- nullable for unpartitioned
    state           TEXT    NOT NULL,           -- PENDING/SUBMITTED/RUNNING/SUCCESS/FAILED
    lsf_job_id      TEXT,
    worker_id       TEXT,                       -- Phase 2
    lease_expires   INTEGER,                    -- Phase 2 (reaper)
    data_version    TEXT,                       -- set by fabric_worker on SUCCESS
    error_message   TEXT,
    submitted_at    INTEGER,
    running_at      INTEGER,
    terminal_at     INTEGER,
    harvested       INTEGER NOT NULL DEFAULT 0  -- harvest sensor flips to 1
);
CREATE INDEX IF NOT EXISTS ix_tasks_state_id  ON tasks(state, id);
CREATE INDEX IF NOT EXISTS ix_tasks_harvested ON tasks(harvested);
```

**狀態機**:
```
PENDING ── dispatch sensor 寫入,等 LSF client 認領
  ↓ lsf_run_client bsub
SUBMITTED ── 已送 LSF,等 CPU
  ↓ (Phase 2: bjobs PEND → RUN)
RUNNING ── LSF 節點上真正在算
  ↓ fabric_worker 寫入
SUCCESS / FAILED ── 終態,harvest sensor 收割
```

**為什麼 SUBMITTED/RUNNING schema 上拆分**:Phase 1 雖然 conflate(不查 bjobs),但欄位先留;Phase 2 加上 bjobs synchronizer 時不用 migration。

#### File lock(`framework/fabric/file_lock.py`)

SQLite + 多 process 並發寫入時的正確性底線。`fcntl.flock(fd, LOCK_EX)` on a sibling `.lock` file:

```python
@contextmanager
def with_write_lock(db_path: Path):
    lock_path = db_path.with_suffix(db_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(f.fileno(), fcntl.LOCK_UN)
```

所有 status_db 寫入都包在這個 context manager 內。

#### LSF client(`framework/fabric/lsf_run_client.py`)

```python
def dispatch(idempotency_key, asset_name, partition_key,
             inner_argv, upstream_data_versions, db_path,
             lsf_queue, lsf_cores, lsf_mem_mb, lsf_walltime,
             fabric_worker_path):
    # 1. INSERT PENDING(UNIQUE 重複自動吸收)
    with file_lock.with_write_lock(db_path):
        status_db.upsert_pending(db_path, idempotency_key,
                                 asset_name, partition_key,
                                 upstream_data_versions)
    # 2. 組 bsub,包 fabric_worker.py 作為實際指令
    bsub_argv = [
        "bsub",
        # NO -K — 非阻塞投遞
        "-J", f"fabric_{idempotency_key[:8]}",
        "-q", lsf_queue, "-n", str(lsf_cores),
        "-R", f"rusage[mem={lsf_mem_mb}]", "-W", lsf_walltime,
        "-o", f"{LOG_DIR}/{idempotency_key}.out",
        "-e", f"{LOG_DIR}/{idempotency_key}.err",
        "-env", "DAGSTER_HOME,PATH,PYTHONPATH",
        sys.executable, str(fabric_worker_path),
        "--db-path", str(db_path),
        "--idempotency-key", idempotency_key,
        "--asset-name", asset_name,
        "--partition-key", partition_key or "",
        "--", *inner_argv,
    ]
    # 3. 投遞,parse job id,寫 SUBMITTED
    proc = subprocess.run(bsub_argv, capture_output=True, text=True, check=True)
    job_id = parse_bsub_output(proc.stderr + proc.stdout)
    with file_lock.with_write_lock(db_path):
        status_db.mark_submitted(db_path, idempotency_key, lsf_job_id=job_id)
```

**鐵則**:無 `-K`,無 wait。asset body 返回後 LSF job 才開始排隊。

#### dagster.fabric.yaml

```yaml
run_launcher:
  module: dagster._core.launcher
  class: DefaultRunLauncher        # NOT 自寫 launcher
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 8         # dispatch/harvest run 都短,設小即可
run_monitoring:
  enabled: true
telemetry: { enabled: false }
# storage: 預設 SQLite at $DAGSTER_HOME(local-sim);Phase 2 切 PostgreSQL
```

### 3.5 M5 — Fabric worker(LSF 節點)

**檔案**:`flows/<flow>/fabric_worker.py`。每個 flow 自己寫一個(因為 data_version 的計算方式與 output artifact 形狀綁定)。

```python
# CLI: fabric_worker.py --db-path … --idempotency-key … \
#                       --asset-name … --partition-key … -- <inner argv>
def main():
    args, inner_argv = parse_args()
    try:
        subprocess.run(inner_argv, check=True)           # set -e 等效
        data_version = _compute_data_version(args)       # flow-specific
        with file_lock.with_write_lock(args.db_path):
            status_db.mark_success(args.db_path, args.idempotency_key,
                                   data_version=data_version)
    except Exception as e:
        with file_lock.with_write_lock(args.db_path):
            status_db.mark_failed(args.db_path, args.idempotency_key,
                                  error_message=str(e)[:1000])
        raise
```

**為什麼 worker 自己算 data_version**:
- Pipes 在大規模 (跨 NFS message file) 增加 I/O 與失敗模式;framework 規模這層冗餘。
- 直接從 output artifact (`.ldb` digest、檔案 SHA、build output hash 等) 算最可靠;不依賴 Dagster runtime。

---

## §4 Worked example:liberate_char

### 4.1 Asset graph 形狀

```
                       start (entry)
                          │ (mapping: all)
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       template_tcl   section_tcl   model_card   netlist   cell_list   main_tcl
       [pvt]          [pvt]         [pvt]        [cell]    -           -
            └──────┬──────┘            │           │         │           │
                   │                   │           │         │           │
                   ▼                   ▼           ▼         ▼           ▼
                 characterize  ◄────────────────────────────────────────────
                 [pvt × cell] = 9 partitions
```

- 6 個 generator:輕量,寫 .tcl/.sp/.csh 檔到 `$LIBERATE_DAG_ROOT/SOURCES/`,emit content_hash data_version。
- 1 個 compute (`characterize`):每個 `[pvt, cell]` 投遞一個 LSF job。

### 4.2 spec.yaml

見 `execution_fabric/flows/liberate_char/spec.yaml`。關鍵欄位:
- `dimensions: { pvt: [tt_25, ff_125, ss_m40], cell: [INV, BUF, NAND2] }` → 9 個 leaf
- `defaults.version: content_hash`
- 每個 generator `partitioned_by: [pvt]` 或 `[cell]`,depends_on `start` with `mapping: all`
- `characterize.depends_on` 用 `mapping: { pvt: identity }`(generator → compute 的維度投影)

### 4.3 script.py(flow owner 的純函數)

見 `execution_fabric/flows/liberate_char/script.py`。
- 6 個 generator function:`gen_<x>(pvt | cell) → dict[abs_path, content]`
- `characterize_command(pvt, cell) → list[str]`(inner argv,**不**包 bsub)

### 4.4 fabric_worker.py(flow-specific 邏輯)

見 `execution_fabric/flows/liberate_char/fabric_worker.py`。`_compute_data_version_from_ldb()` 讀 `.ldb` 檔的 `digest` 行——對 liberate 來說這是 deterministic 的 build 輸出指紋。

### 4.5 端到端追蹤一個 partition

1. **t=0**:dispatch sensor 第一次 tick → 看到 `desired={(tt_25,INV), …}` 全部、`observed=∅` → 9 個 RunRequest 排隊。
2. **t=5s**:Dagster daemon 啟動 9 個短命 run。每個 run 的 asset body:
   - 計算 `idem_key = sha256("characterize|tt_25/INV|" + sorted(upstream dvs))`
   - `lsf_run_client.dispatch(...)` → bsub 出去,parse job_id,status DB 從 PENDING → SUBMITTED
   - asset body return None;Dagster 自動 emit placeholder materialization(無真實 data_version)
3. **t=10–60s**:9 個 fabric_worker 在 LSF 節點上跑(實際是 mock bsub 在本地 fork 起來)。每個 worker 算完 → `_compute_data_version_from_ldb(.ldb)` → status DB SUCCESS + data_version。
4. **t=90s**:harvest sensor tick → 看到 9 個 unharvested SUCCESS → 對每個 emit `report_runless_asset_event(AssetMaterialization(tags={'dagster/data_version': real_digest}))` → mark_harvested → cursor 推進。
5. **下次 dispatch sensor tick**:`observed = {(tt_25,INV), …}` 9 個,`desired - observed = ∅`,不再 redispatch。

### 4.6 跑起來

```tcsh
setenv DAGSTER_HOME /tmp/liberate-char-dagster-home
setenv LIBERATE_DAG_ROOT /tmp/liberate-char-dag
cd /home/user/Dagster-expert/execution_fabric
python -m scripts.run_demo
```
(bash: `export DAGSTER_HOME=… ; export LIBERATE_DAG_ROOT=…`)

預期:5 分鐘內 9 個 `.ldb` 在 `$LIBERATE_DAG_ROOT/out/`、9 個 SUCCESS 在 status DB、9 個 AssetMaterialization 在 Dagster event log。

---

## §5 Migration plan — 把既有 script 移植進框架

**方法論**:**TDD + Clean Code**。每一步都有 RED → GREEN → REFACTOR 三段。

### 5.1 第一步:Identify the work units

回答四個問題,落筆寫下答案。

1. **Assets**:既有 pipeline 產出哪些 *類型* 的檔案?每一類 = 一個 asset。
   - 例(liberate_char):template tcl / section tcl / model card / netlist / cell list / main tcl / .ldb output → 7 個 asset(6 generator + 1 compute leaf)。
2. **Dimensions**:既有 script 的 loop 變數是什麼?每個 loop 變數 = 一個 partition dimension。
   - 例:`for pvt in tt_25,ff_125,ss_m40` + `for cell in INV,BUF,NAND2` → 2 維。
3. **Dependencies**:哪些 asset 餵哪些?用 `depends_on.mapping` 表達維度投影(`identity` / `all` / `last`)。
4. **Cardinality math first**:總 leaf 數 = ∏ dimensions。
   - 例:3 × 3 = 9。若 > 數萬,先拆分階(`learn/12-scaling/` 的記憶教訓——「dimensions × steps × cells × runs」很快爆炸)。

### 5.2 第二步:Write the spec(RED test first)

```python
# tests/test_<flow>_spec.py
def test_<flow>_graph_shape():
    spec = load_spec("flows/<flow>/spec.yaml")
    assert {a.name for a in spec.assets} == {"start", "g1", "g2", …, "leaf"}
    leaf = next(a for a in spec.assets if a.kind == "compute")
    assert sorted(leaf.partitioned_by) == sorted(["dim1", "dim2"])
    assert {d.asset for d in leaf.depends_on} == {"g1", "g2", …}
```

寫完測試 → 改 `spec.yaml` → `pytest`。GREEN 時表示 graph 形狀對。

### 5.3 第三步:Extract pure functions(Clean Code)

每個 compute / generator 一個 callable,**簽名固定**:
- generator:`fn(*partition_values) → dict[abs_path, content]`
- compute:`fn(*partition_values) → list[str]`(inner argv,**不**包 bsub)

**Clean Code 原則**:
- I/O 與邏輯分開:純函數不 read/write file system;檔案讀寫由 framework 處理。
- 沒有 dagster import(`grep -E "^(from|import) dagster" script.py` 必須 0 命中)。
- 沒有全域副作用(no module-level mutation)。

```python
# tests/test_<flow>_script.py
from flows.<flow>.script import gen_template, characterize_command

def test_gen_template_returns_path_content_dict():
    out = gen_template("tt_25")
    assert isinstance(out, dict)
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in out.items())

def test_characterize_command_inner_argv_has_no_bsub():
    argv = characterize_command("tt_25", "INV")
    assert "bsub" not in argv  # framework 那層才加 bsub
```

### 5.4 第四步:Wire fabric_worker for THIS flow

```python
# flows/<flow>/fabric_worker.py
def _compute_data_version_from_outputs(args, inner_argv) -> str:
    """Read the artifact <flow> produced; return a stable digest."""
    # 例:讀 .ldb 的 digest 行、檔案 SHA256、build hash 都可以。
    ...
```

`fabric_worker.py` 的 main() 可以 reuse `framework/fabric/_worker_main.py` 的 boilerplate (CLI parse → subprocess.run → mark success/failed)。

### 5.5 第五步:End-to-end demo

```python
# execution_fabric/scripts/run_demo.py
def main():
    setup_env()                                 # DAGSTER_HOME + flow-specific roots
    launch_daemon_in_thread()
    wait_until(lambda: count_success(db_path) == EXPECTED, timeout=300)
    assert count_materializations(asset_key) == EXPECTED
    assert all_output_artifacts_exist()
```

### 5.6 第六步:TDD priority order(機械化檢查)

寫測試的順序,先寫高層,後寫底層;先寫脆弱的,後寫穩定的:

| 順位 | 測試 | 為什麼這個順序 |
|---|---|---|
| 1 | `test_<flow>_spec.py` | 最便宜;graph 錯掉所有下游都白費 |
| 2 | `test_<flow>_script.py` | 純函數;flow owner 自己寫,bug 最多 |
| 3 | `test_status_db.py` | framework 的;DB 是執行真相 |
| 4 | `test_file_lock.py` | framework 的;併發正確性底線 |
| 5 | `test_lsf_run_client.py` | framework 的;argv 組裝 + bsub mock |
| 6 | **`test_harvest_sensor.py`** | **最脆弱**;over-test cursor + idempotency + restart + partial failure |
| 7 | `test_dispatch_sensor.py` | framework 的;desired−observed |
| 8 | `scripts/run_demo.py` | 整合測試;通常一發就抓到剩下 bug |

### 5.7 從舊 pipeline 該刪什麼

把這些既有元件**拆掉**——它們在新模型下變成噪音或衝突:

- 手寫 scheduling / cron / dependency walker → Dagster 取代
- 「這個工作有沒有做過」自查邏輯 → status DB 取代
- 用檔案存「進行中」狀態 → status DB 取代
- 重試 / retry queue → Phase 2 reaper 取代;Phase 1 接受手動重啟

---

## §6 User 中斷 & 重跑處理

### 6.1 User Ctrl+C 中斷 demo

- **發生什麼**:daemon 死,dispatch sensor 與 harvest sensor 停止 tick。**已經 bsub 出去的 fabric_worker 不受影響**——它們在 LSF 節點上獨立執行,完成後仍會寫 status DB。
- **重啟後**:dispatch sensor 從 status DB 讀 `observed`(任何 SUCCESS 都算),不重投。harvest sensor 從 cursor 繼續。
- **Phase 1 限制**:中斷瞬間正卡在 SUBMITTED 但 worker 死掉(例如 LSF 殺 job)的 row 會永遠卡住。需要手動 SQL `UPDATE tasks SET state='PENDING', lsf_job_id=NULL WHERE …`,或刪 row 讓 dispatch sensor 重投。

### 6.2 User 取消特定 partition

```tcsh
sqlite3 $STATUS_DB "SELECT lsf_job_id FROM tasks \
  WHERE asset_name='characterize' AND partition_key='tt_25|INV'"
bkill <job_id>
sqlite3 $STATUS_DB "UPDATE tasks SET state='FAILED', \
  error_message='user cancelled' WHERE …"
```

Phase 2:control plane 寫 CANCELLED 狀態;dispatch sensor 把 CANCELLED 視為「已 observed」不重投;UI 提供按鈕。

### 6.3 User 重跑 partition(input 沒變)

- 觸發指紋 = `sha256(asset, partition, sorted(upstream_data_versions))`。
- Input 沒變 → 上游 data_version 沒變 → trigger fingerprint 相同 → idempotency_key 相同。
- `status_db.upsert_pending(...)` 在 UNIQUE constraint 上撞牆——old row 已是 SUCCESS;新的 INSERT 被吸收(no-op)。
- **沒有重複投遞,沒有重複計算**。

### 6.4 User 重跑 partition(input 變了)

- 改 upstream 的 generator script 或 input data → 新的 content_hash data_version。
- 觸發指紋變 → 新 idempotency_key。
- dispatch sensor 看 `observed`(舊 SUCCESS 的 idem_key)≠ 新 idem_key → 「missing」 → 重新 dispatch。
- 舊 SUCCESS row 留在 DB(歷史)。harvest sensor 報新的 materialization;Dagster 的 latest-wins 用最新的 data_version 推進 lineage。
- Phase 2:TTL / archival 清舊 row。

### 6.5 Phase 1 明確不處理的情況(接受限制)

| 情況 | Phase 1 行為 | Phase 2 怎麼解 |
|---|---|---|
| Worker 在 bsub 後、mark_success 前死掉 | row 永遠 SUBMITTED | bjobs-driven reaper |
| LSF job 從 PEND → RUN 的轉換 | status DB 一直顯示 SUBMITTED | LSF synchronizer |
| 多 host 共享 status DB | NFS + SQLite + file lock(小規模 OK,正式上量不行) | PostgreSQL |
| 數萬 worker 同時撞 status DB | SQLite 寫入 serialize 成瓶頸 | PostgreSQL + 連線池 |

---

## §7 Critical correctness

### 7.1 Idempotency key — 一把鑰匙開三道門

```python
idempotency_key = sha256(
    f"{asset_name}|{partition_key}|" +
    "|".join(sorted(upstream_data_versions))
).hexdigest()
```

- **故障一(投遞去重)**:UNIQUE constraint;同 key 第二次 INSERT 自動 no-op。
- **故障二(孤兒回收,Phase 2)**:同 key 不變,reaper 移除舊 row 重投,Dagster 視為同一邏輯 task。
- **故障三(收割冪等)**:harvest sensor 即使重複 report,Dagster 的 event log latest-wins 確保身分一致。

「觸發指紋包含 upstream data_versions」是**關鍵設計**:
- 上游沒變 → 重複觸發被吸收(避免不必要的重算)。
- 上游變了 → 新 key,視為新任務(正確的 reactive 行為)。

### 7.2 SUBMITTED / RUNNING 拆分(Phase 1 schema-ready)

Phase 1 不查 bjobs,SUBMITTED + RUNNING 合二為一;但 schema 上**先預留兩個欄位**:
- `submitted_at` — bsub 返回時寫
- `running_at` — Phase 2 由 bjobs synchronizer 寫

Phase 2 啟動 synchronizer 時,直接 UPDATE 而非 schema migration。

### 7.3 Harvest sensor — 最該優先 TDD 釘死

必須覆蓋的測試案例:

| 案例 | 行為 |
|---|---|
| 重複 tick(SUCCESS 已 harvested) | cursor 不動,SkipReason no progress |
| sensor 重啟在 batch 中段 | cursor 停在 last successful row,下次重做這批 |
| `report_runless` 中段拋例外 | break, cursor 不動,下 tick 重試 |
| 大量(>200)unharvested | 分批處理(limit=200),每批一個 cursor 推進 |
| delete-after-harvest 失敗 | harvested=1 標記後 cleanup loop 處理(不阻 sensor) |

詳細測試模板見 `execution_fabric/tests/test_harvest_sensor.py`。

---

## §8 Phase 1 vs Phase 2 範圍對照

| 元件 | Phase 1(本版) | Phase 2(最終目標) |
|---|---|---|
| Status DB | SQLite + file lock | PostgreSQL |
| 投遞 | dispatch sensor 直寫 PENDING | + Kafka priority queue |
| LSF dispatch | 非阻塞 bsub(無 -K) | 同 |
| Fabric worker | 同節點 subprocess | 同 + heartbeat |
| LSF 狀態同步 | conflated(SUBMITTED 就停留) | bjobs synchronizer(SUBMITTED/RUNNING) |
| 孤兒回收 | 手動 SQL | reaper + bjobs |
| 投遞去重 | UNIQUE constraint | 完整 §6.1 contract |
| 收割冪等 | cursor + idempotent | 同 |
| Worker pool | 無(LSF 直接收) | 動態擴縮(resource control) |
| Control logic | 無(手動 SQL) | event/resource control + UI |
| Event log | 無(Phase 2) | append-only 獨立表 |

---

## §9 Acceptance criteria

完成本架構應滿足:

1. **Demo 通過**:`cd execution_fabric && python -m scripts.run_demo`
   - 9 個 RunRequest 被 dispatch sensor emit
   - 9 個 SUCCESS row 在 status DB
   - 9 個 `.ldb` 檔案在 `$LIBERATE_DAG_ROOT/out/`
   - 9 個 `AssetMaterialization` event 在 Dagster event log(harvest sensor 寫的)
   - 端到端 < 5 分鐘
2. **測試**:`cd execution_fabric && pytest -xvs` 全綠。
3. **靜態檢查**:
   - `grep -rn "LSFRunLauncher" execution_fabric/` 0 命中(push 模型死絕)
   - `grep -rn "PipesSubprocessClient" execution_fabric/framework/` 0 命中(framework 不依賴 Pipes)
   - `grep -E "^(from|import) dagster" execution_fabric/flows/*/script.py` 0 命中
4. **中斷重跑**:§6.1 / §6.3 / §6.4 場景手動跑過、行為符合描述。
5. **Dagster 版本**:`dagster --version` 顯示 1.13.7。

---

## §10 Appendix

### 10.1 完整 schema.sql

見 `execution_fabric/framework/fabric/schema.sql`。

### 10.2 Glossary

| 詞 | 定義 |
|---|---|
| **Idempotency key** | `sha256(asset, partition, sorted(upstream data_versions))`;status DB UNIQUE column |
| **Trigger fingerprint** | 同 idempotency key;舊文件用詞,語義一樣 |
| **Dispatch sensor** | 投遞線:reading `desired − observed`,寫 PENDING + emit RunRequest |
| **Harvest sensor** | 收割線:reading status DB SUCCESS,emit `report_runless_asset_event` |
| **Fabric worker** | 在 LSF 節點上的 subprocess,跑 inner script + 自算 data_version + 寫 SUCCESS |
| **Observed**(dispatch sensor) | Status DB 中該 asset 的 SUCCESS partition keys(**不**是 Dagster materializations) |
| **Desired**(dispatch sensor) | `partitions_def.get_partition_keys()` |
| **Runless asset event** | Dagster 1.13.7 的 `DagsterInstance.report_runless_asset_event(AssetMaterialization(...))`;讓 sensor 不透過 run 直接更新 lineage |

### 10.3 不變量總表

1. 同一 idempotency_key 在 status DB 中最多一筆 active(PENDING/SUBMITTED/RUNNING)。
2. 任何 SUCCESS row 最終都會被 harvest sensor 處理(at-least-once);harvested=1 後 cursor 不會倒退。
3. harvest sensor 的 cursor 推進 ONLY AFTER 全部 `report_runless` 成功 + `mark_harvested` 成功。
4. dispatch sensor 的 `observed` 來自 status DB(不來自 Dagster materializations)。
5. Fabric worker 失敗時,raise 之前必先 `mark_failed`。

### 10.4 1.13.7 sensor API 探測

`DagsterInstance.report_runless_asset_event(AssetMaterialization(asset_key, partition, tags))` 在 1.13.7 仍存在、語義不變(latest-wins、append-only)。`@sensor` body 可呼叫此方法並 `return SkipReason`,side effect 仍生效——這是 harvest sensor 的核心契約。

實作前必跑:
```tcsh
python -c "
from dagster import DagsterInstance, AssetMaterialization, AssetKey
inst = DagsterInstance.ephemeral()
inst.report_runless_asset_event(AssetMaterialization(asset_key=AssetKey('test'), tags={'dagster/data_version':'v1'}))
print('OK, runless event works in 1.13.7')
"
```
