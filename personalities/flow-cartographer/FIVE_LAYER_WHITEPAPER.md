# 五層通用 Dagster 框架 — 實作白皮書

> **本文件的用途**：交給實作 agent（Kimi 2.5T）作為建構規格。
> **實作對象**：一個 spec-driven 的五層 Dagster 框架，flow owner 只提供 script + data version + YAML spec，框架自動產生 assets / partitions / mappings / sensors，並把運算投遞到 LSF。
> **環境**：air-gapped、CentOS 7、LSF、tcsh、NFS、Dagster 1.13.x（asset-centric）。
> **驗證方式**：用既有的 `netlist_files`（trio_group × cell 降維案例）作為第一個 reference flow，跑通端到端。
>
> **D1 狀態（2026-06-04，已驗證）**：本架構已用 **liberate-char**（pvt × cell，
> 6 generator + 9 characterize leaf）作第一個 application 實作,並以真 Dagster
> 1.13.3 的 **daemon + reconcile sensor** 端到端跑通(9/9 leaf,內容 digest 與
> hand-rolled 參考實作 **MATCH**)。可運行 reference 在
> `spec_dagster/`(repo top-level;`python -m scripts.run_demo`);
> 建構過程踩到的 API 雷與據此回修本白皮書的項目,見該目錄 `LESSONS.md`。本文中
> 標「(D1 實證/補)」的段落即來自該次實作。

---

## 0. 給實作 agent 的總指示（先讀這段）

你要建構的不是一個 pipeline，而是一個**框架**：它讀 YAML spec，程式化（非 hardcode、非逐 flow 手寫）產生 Dagster 物件。實作時請嚴格遵守以下原則，違反任一條都算實作失敗：

1. **分層職責不可混**。五層各有單一職責（見 §1）。M2/M3 是「讀 spec 生成」，M4/M5 是「框架現成元件 + spec 餵參數」。
2. **flow owner 永遠不手寫 partition mapping**。mapping 由 §4 的 `build_mapping()` 從 spec 的意圖宣告產生，方向正確性由單元測試保證。
3. **sensor 是 reconciliation 模型**（desired − observed），狀態存 event log，cursor 只存指紋。禁止 append-only watermark。見 §5。
4. **LSF 走自寫 `LSFRunLauncher`（M4)**，一個 Dagster run = 一個 bsub = 一個 LSF job。asset body 內【絕不再】bsub（那才是 nested bsub）。Pipes 降為 M5 同節點選用收集器（收 EDA 工具 stdout / 報 materialization）；命令內禁含 bsub。**為何自寫**：真實內網 >10k 同時 run request，orchestrator 無法 fork >10k worker process，必須卸載到 LSF。小規模（<~hundreds of runs）仍可走 DefaultRunLauncher + asset 內 Pipes bsub，見 `personalities/dagster-expert/learn/13-lsf-integration/` Part A。見 §6。
5. **run_key 用 `hashlib`，不用 `hash()`**（跨 process 穩定）。
6. **每個模組先寫測試再寫實作（TDD）**。純函數（mapping builder、grouping planner、version 計算）必須有獨立的 pytest，不依賴 Dagster runtime、不碰 LSF、不碰檔案系統。
7. **DAGSTER_HOME 錨在本機磁碟**，不放 NFS。run/event/schedule store 走 **PostgreSQL**（遠端 LSF worker 必須共享狀態；SQLite-on-NFS 不可用）；local-sim 才用 SQLite。詳見 §6.1。
8. **不要過度設計**。只實作 reference flow（netlist_files）跑通所需的東西。spec schema 預留擴充欄位但不實作未用到的分支。標記 `# FUTURE:` 註解，不寫空殼。

交付順序見 §9 的里程碑。每個里程碑結束都要能 demo、能跑測試。

---

## 1. 架構總覽：五層職責

```
┌─────────────────────────────────────────────────────────────┐
│  M1  Spec 層（YAML)                                            │
│  職責：宣告 work units、dependencies、batching、LSF resources  │
│  誰維護：flow owner                                            │
│  產物：flows/<name>/spec.yaml                                  │
└───────────────────────────┬─────────────────────────────────┘
                            │ generator 讀取
┌───────────────────────────▼─────────────────────────────────┐
│  M2  Definitions 產生器                                        │
│  職責：spec → assets / partitions / mappings / jobs            │
│  誰維護：framework owner（你 + agent)                          │
│  關鍵：partition mapping 程式化產生,方向正確,有測試            │
└───────────────────────────┬─────────────────────────────────┘
                            │ 產出 Dagster 物件
┌───────────────────────────▼─────────────────────────────────┐
│  M3  Grouping Sensor 層                                        │
│  職責：reconciliation（desired−observed)→ 分批 → RunRequest   │
│  誰維護：framework owner                                       │
│  關鍵：純決策函數 plan_batches 可獨立測試                       │
└───────────────────────────┬─────────────────────────────────┘
                            │ 觸發 run（>10k RunRequest)
┌───────────────────────────▼─────────────────────────────────┐
│  M4  Launch 層：自寫 LSFRunLauncher（RunLauncher 子類)        │
│  職責：1 Dagster run = 1 bsub = 1 LSF job                     │
│        launch_run() 投 `dagster api execute_run`;             │
│        terminate() = bkill;                                   │
│        check_run_worker_health() = bjobs                      │
│  誰維護：framework owner（一次性元件,所有 flow 共用)          │
│  為何自寫：>10k 同時 run,orchestrator 無法 fork >10k worker    │
│           process,必須把 worker 卸載到 LSF                     │
└───────────────────────────┬─────────────────────────────────┘
       bsub 投到 LSF node    │      ▲ 共享 backend(見下)
┌───────────────────────────▼──────┴──────────────────────────┐
│  M5  Run worker @ LSF node：in_process executor              │
│  職責：在 LSF node 上跑該 run 的 asset graph(in_process),     │
│        循序處理該批 items;(可選)同節點 PipesSubprocessClient  │
│        收 EDA 工具 stdout / 結構化 materialization            │
│  誰維護：framework owner                                       │
│  鐵則：Pipes 命令內【絕不可】再出現 bsub(否則 nested bsub)     │
└──────────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────────────────┐
   │  跨層 backend(M4/M5 共享狀態的接縫,§6.1 dagster.yaml)    │
   │  storage:可插拔 — local-sim 用 SQLite;prod 用 Postgres   │
   │           (遠端 LSF worker 必須共享 run/event/schedule)   │
   │  coordinator:QueuedRunCoordinator,max_concurrent_runs    │
   │           = min(LSF queue 容量, Postgres 連線預算)        │
   │  run_monitoring:用 launcher 的 check_run_worker_health    │
   │           偵測 LSF job 死亡 → 自動標 run 失敗             │
   └──────────────────────────────────────────────────────────┘
```

**核心原則：宣告與執行分離。** M1 是「宣告什麼」（flow owner、YAML）；M2–M5 是「如何執行」（framework、程式）。執行的「跨主機接縫」是 backend（共享 storage + coordinator）：M4 在 orchestrator 上只負責投遞，M5 在 LSF node 上負責執行，兩者靠 Postgres 對齊狀態。

---

## 2. 專案結構（package 組織）

採 **framework by-layer + flows by-feature** 的混合（理由見本系列討論）：

```
repo/
  framework/                      # framework owner 維護,by-layer
    __init__.py
    generator.py                  # M2 入口:build_definitions(flows_dir)
    spec/
      schema.py                   # M1 spec 的 Pydantic schema + 驗證
      loader.py                   # 掃描 flows/*/spec.yaml,載入並驗證
    assets/
      builder.py                  # M2:build_asset(spec, script, version_fn)
      mapping_builder.py          # M2:build_mapping() — 方向正確的核心
      partition_builder.py        # M2:build_partitions_def()
    sensors/
      factory.py                  # M3:build_sensor(spec)
      planner.py                  # M3:plan_batches() 純函數
      reconcile.py                # M3:desired/observed 查詢 + ReadinessSource
    versioning/
      base.py                     # ② data version 基礎版 + interface
    launcher/
      lsf_run_launcher.py         # M4:自寫 LSFRunLauncher(RunLauncher 子類)
      bsub.py                     # M4:bsub / bjobs / bkill 組裝,獨立可測
    pipes/
      same_node.py                # M5:同節點 PipesSubprocessClient helper(選用,
                                  #     收 EDA 工具 stdout / materialization;命令禁含 bsub)
    config/
      dagster.yaml                # M4:launcher / executor / concurrency
    tests/                        # 框架本身的測試
      test_mapping_builder.py
      test_planner.py
      test_versioning.py
      test_spec_schema.py

  flows/                          # flow owner 維護,by-feature
    netlist/                      # 第一個 reference flow
      spec.yaml                   # M1
      script.py                   # ① flow owner 的 script
      data_version.py             # ②（可選,選基礎版則省略)
      tests/
        test_script.py
    _template/                    # 給新 flow owner 複製的範本
      spec.yaml
      script.py

  definitions.py                  # 入口:defs = build_definitions("flows/")
  pyproject.toml                  # pip install -e . 用
  README.md
```

**[實作契約]** `framework/` 不得 import `flows/` 的任何具體 flow（框架不認識具體 flow）。依賴方向永遠是 `flows/` 的 spec 被 `framework/` 的 generator 讀取，而非反向。

---

## 3. M1 — Spec Schema（YAML）

### 3.1 Spec 範例（reference flow: netlist_files）

```yaml
# flows/netlist/spec.yaml
version: 1
flow_name: netlist

# 維度軸定義
dimensions:
  trio_group:
    type: static
    values: [g0, g1, g2]          # 或 type: dynamic, source: <resource>
  cell:
    type: dynamic
    source: cell_registry          # work_items 用,不進 partition

# spec 級預設 — kind 與 dispatch 解耦,小規模或 dev 用 local,大規模才改 lsf
defaults:
  dispatch: lsf                    # M4 LSFRunLauncher 接管;asset body 內絕不再 bsub
  version: content_hash

# work units → assets
assets:
  - name: start
    kind: entry                    # 無 partition、無上游的起點
    partitioned_by: []

  - name: netlist_files
    kind: compute                  # entry / generator / compute(不再有 lsf_compute)
    script: flows.netlist.script:run_netlist   # ① 指向 flow owner script
    partitioned_by: [trio_group]   # 降維:只用粗維度
    work_items: cell               # cell 不當 partition,走 config-carried batch
    depends_on:
      - asset: start
        mapping: all
    # dispatch / lsf 可 per-asset override;此例繼承 defaults

# batching 策略
batching:
  default: { strategy: fixed, size: 100 }
  overrides: {}                    # 例:netlist_files: { size: 50 }

# LSF 資源 — dispatch=lsf 的 asset 必須有 queue/cores/mem_mb/walltime
# (schema 載入時驗證;launcher 從 RunRequest.tags["lsf/*"] 讀)
lsf:
  default: { queue: normal, cores: 4, mem_mb: 8192, walltime: "24:00" }
  overrides: {}                    # 例:netlist_files: { mem_mb: 16384 }
```

### 3.2 Spec Schema（Pydantic,framework/spec/schema.py）

**[實作契約]** 用 Pydantic v2 定義 schema，載入時嚴格驗證。關鍵欄位：

```python
from pydantic import BaseModel, Field
from typing import Literal

# kind 與 dispatch 解耦:同樣的 compute,小規模 dispatch=local,大規模 dispatch=lsf
Dispatch = Literal["local", "lsf"]

class DimensionSpec(BaseModel):
    type: Literal["static", "dynamic"]
    values: list[str] | None = None        # static 必填
    source: str | None = None              # dynamic 必填,指向 resource

class DependencySpec(BaseModel):
    asset: str
    # mapping 的值是「意圖」,不是 Python。generator 翻譯成方向正確的 PartitionMapping
    mapping: str | dict[str, str] = "identity"
    #   "all"                          → AllPartitionMapping
    #   "last"                         → LastPartitionMapping
    #   {"trio_group": "identity",     → MultiPartitionMapping（逐維)
    #    "pvt": "all_of(cell)"}

class AssetSpec(BaseModel):
    name: str
    kind: Literal["entry", "generator", "compute"]   # generator:輕量轉換;compute:重 EDA
    script: str | None = None              # "module.path:callable"
    version: str = "content_hash"          # 基礎版名 或 "module:callable"
    partitioned_by: list[str] = Field(default_factory=list)
    work_items: str | None = None          # 降維用的細維度名
    depends_on: list[DependencySpec] = Field(default_factory=list)
    dispatch: Dispatch | None = None       # per-asset override(若空則用 spec.defaults.dispatch)
    lsf: "LSFResource | None" = None       # per-asset override(若空則用 spec.lsf.default)

class BatchingRule(BaseModel):
    strategy: Literal["fixed"] = "fixed"   # FUTURE: weighted, timeout
    size: int = 100

class LSFResource(BaseModel):
    # M4 launcher 從 RunRequest tags(lsf/queue, lsf/cores, ...) 讀這些值;
    # 移除 poll_interval_s — 輪詢交給 run_monitoring.poll_interval_seconds daemon-wide
    queue: str = "normal"
    cores: int = 4
    mem_mb: int = 4096
    walltime: str = "24:00"                # HH:MM
    project: str | None = None             # 選用,billing/quota 識別碼

class FlowDefaults(BaseModel):
    trigger: Literal["automation", "reconciliation"] | None = None
    dispatch: Dispatch = "local"           # 預設 local,大規模才改 lsf
    version: str = "content_hash"

class FlowSpec(BaseModel):
    version: int
    flow_name: str
    dimensions: dict[str, DimensionSpec]
    defaults: FlowDefaults = Field(default_factory=FlowDefaults)
    assets: list[AssetSpec]
    batching: dict[str, ...] = Field(default_factory=lambda: {"default": BatchingRule()})
    lsf: dict[str, ...] = Field(default_factory=dict)   # {default: LSFResource, overrides: {...}}
```

**[實作契約] 驗證規則（schema 載入時就擋掉錯誤,不要等 runtime）：**
- `mapping` 宣告裡引用的維度名，必須存在於上游或下游的 `partitioned_by`。
- `script` 指向的 callable 必須 importable（載入時嘗試 import，失敗即報錯）。
- `work_items` 的維度名必須在 `dimensions` 裡定義為 `dynamic`。
- `depends_on` 的 asset 必須存在於同 spec 的 assets。
- **若 任一 compute asset 的 effective `dispatch == "lsf"`（asset 自帶或繼承自 `defaults.dispatch`），則 effective `lsf` 區塊（per-asset override 或 `spec.lsf.default`）必須齊備 `queue`、`cores`、`mem_mb`、`walltime` 四欄。** 缺一即載入失敗（不要等 launcher 投 bsub 才報錯）。
- 跑 `test_spec_schema.py`：餵合法/非法 spec，驗證該過的過、該擋的擋。

### 3.5 Onboarding SOP — flow owner 怎麼上一個新 flow

flow owner 只交付三個檔（外加 framework 樣板生成的 `definitions.py` + `workspace.yaml`）。
以下 7 步是機械化流程，弱-agent 也能逐步跑（對齊 MEMORY.md「mechanical triggers」偏好）。

```
0. 前置(framework owner 已就位):
   - Postgres + (選)PgBouncer 開好(prod);local-sim 用 SQLite 即可
   - dagster.yaml 兩份在指定 $DAGSTER_HOME(dagster.prod.yaml / dagster.localsim.yaml,見 §6.1)
   - framework package 已 `pip install -e .` 可 import

1. 拆解 flow:
   - dimensions:列出每個維度(static/dynamic),做「cardinality math first」
     算總葉子數(MEMORY.md 偏好)
   - assets 分類:entry(無上游起點)/ generator(輕量轉換)/ compute(重 EDA)
   - 依賴 mapping:per-dimension(identity / all / last / all_of)
   - 每個 compute:dispatch=local(<~hundreds)還是 lsf(>~thousands);資源(queue/cores/mem/wall)

2. 寫 flows/<name>/spec.yaml:
   - 對齊 §3.1 範本與 §3.2 schema
   - 載入驗證:`python -c "from framework.spec.loader import load_all; load_all('flows/<name>')"`
     退 0;故意改錯欄位(`dispatch: lsf` 但缺 `lsf.default`)確認載入時就報明確錯

3. 寫 flows/<name>/script.py(純函數,每個 asset 一個 callable):
   - 簽名 = framework builder 規定的契約(見 §4.3 _asset body)
   - `grep -E "^(from|import) dagster" script.py` 必須 0 命中
   - `grep -E "@(asset|sensor)" script.py` 必須 0 命中
   - **命令絕不含 bsub**(launcher 已做;在這裡再 bsub 是 nested = 違反 §0 第 4 條)

4. 寫 flows/<name>/definitions.py(一行):
   `from framework.generator import build_definitions; defs = build_definitions("flows/<name>/")`
   + 寫 flows/<name>/workspace.yaml 指向 definitions

5. 選 DAGSTER_HOME(對齊 §6.1 兩份 dagster.yaml):
   - prod:`setenv DAGSTER_HOME /local/dagster_home/prod`(LSFRunLauncher + Postgres)
     (bash: `export DAGSTER_HOME=/local/dagster_home/prod`)
   - local-sim:`setenv DAGSTER_HOME /local/dagster_home/localsim`(DefaultRunLauncher
     + SQLite + mock bsub on PATH)

6. 跑起來:
   - dev:`dagster dev -w flows/<name>/workspace.yaml`
   - prod:`dagster-daemon run &`(daemon)+ `dagster-webserver` 部署到既有 instance

7. 觀察(prod):
   - UI 看 asset graph、partition status
   - `bjobs -u $USER` 看 LSF in-flight runs
   - `psql -c "select count(*) from pg_stat_activity where datname='dagster'"`
     確認連線數在預算內(§6.1 公式)
```

**Spec MVP 必填 vs 選用對照表**（schema 之外的人類速查）：

| 場合 | 必填欄位 |
|---|---|
| 任何 flow | `version`、`flow_name`、`dimensions(>=1)`、`assets(>=1)`、每 asset 的 `name`、`kind` |
| `kind: compute` | `script`、effective `dispatch`（自帶或繼承 `defaults.dispatch`） |
| 有任一 asset `dispatch: lsf` | `lsf.default.{queue,cores,mem_mb,walltime}` 或 per-asset `lsf.{...}` |
| 選用 | `version`（預設 `content_hash`）、`partitioned_by`、`work_items`、`depends_on`、`batching.overrides`、`lsf.project`、per-asset `lsf` override、`op_tags` |

---

## 4. M2 — Definitions 產生器

### 4.1 mapping_builder.py（最關鍵、最容易錯的模組）

**[實作契約]** 這是整個框架方向正確性的單一保證點。`MultiPartitionMapping` 的方向反直覺：**dict key = upstream 維度名，`DimensionPartitionMapping.dimension_name` = downstream 維度名**。這個方向由本函數封裝，呼叫端永不需要知道。

```python
import dagster as dg

def build_mapping(
    mapping_spec: str | dict[str, str],
    upstream_dims: list[str],
    downstream_dims: list[str],
) -> dg.PartitionMapping | None:
    """從 spec 的 mapping 意圖,產出方向正確的 PartitionMapping。

    上下游維度相同且 mapping=identity → 回 None（Dagster 自動 identity)。
    """
    if mapping_spec == "all":
        return dg.AllPartitionMapping()
    if mapping_spec == "last":
        return dg.LastPartitionMapping()
    if mapping_spec == "identity" and set(upstream_dims) == set(downstream_dims):
        return None                              # 省略,讓 Dagster 自動處理

    # 維度級對應:mapping_spec 是 {downstream_dim: rule}
    assert isinstance(mapping_spec, dict)
    dimension_mappings: dict[str, dg.DimensionPartitionMapping] = {}
    for down_dim, rule in mapping_spec.items():
        if rule == "identity":
            up_dim, inner = down_dim, dg.IdentityPartitionMapping()
        elif rule.startswith("all_of("):
            up_dim = rule[len("all_of("):-1].strip()      # all_of(cell) → cell
            inner = dg.AllPartitionMapping()
        elif rule.startswith("static("):
            up_dim, mp = _parse_static(rule)
            inner = dg.StaticPartitionMapping(mp)
        else:
            raise ValueError(f"unknown mapping rule: {rule!r}")

        # 方向關鍵:dict key = UPSTREAM 維度名
        dimension_mappings[up_dim] = dg.DimensionPartitionMapping(
            dimension_name=down_dim,             # = DOWNSTREAM 維度名
            partition_mapping=inner,
        )
    return dg.MultiPartitionMapping(dimension_mappings)
```

**[實作契約] test_mapping_builder.py 必測案例：**
```python
def test_all():                # mapping="all" → AllPartitionMapping
def test_identity_same_dims(): # 上下游同維 + identity → None
def test_identity_plus_all_of():
    # 上游 [trio_group, cell],下游 [trio_group, pvt]
    # {"trio_group":"identity", "pvt":"all_of(cell)"}
    m = build_mapping({"trio_group":"identity","pvt":"all_of(cell)"},
                      ["trio_group","cell"], ["trio_group","pvt"])
    assert isinstance(m, dg.MultiPartitionMapping)
    # 驗證 dict key 是 upstream 維度名(trio_group, cell),不是 downstream
    keys = set(m.downstream_mappings_by_upstream_dimension.keys())
    assert keys == {"trio_group", "cell"}
def test_static_rename():      # static(...) 兩端 static、key 不同名
def test_unknown_rule_raises():
```

### 4.2 partition_builder.py

```python
def build_partitions_def(asset_spec, dimensions) -> dg.PartitionsDefinition | None:
    """從 partitioned_by 建 partition def。work_items 維度不納入(降維)。"""
    dims = asset_spec.partitioned_by          # 注意:不含 work_items
    if not dims:
        return None                            # entry asset 無 partition
    if len(dims) == 1:
        return _single_dim(dimensions[dims[0]])
    # 多維 → MultiPartitionsDefinition（注意:只能一個 dynamic 維度)
    return dg.MultiPartitionsDefinition({
        d: _single_dim(dimensions[d]) for d in dims
    })

def _single_dim(dim_spec) -> dg.PartitionsDefinition:
    if dim_spec.type == "static":
        return dg.StaticPartitionsDefinition(dim_spec.values)
    return dg.DynamicPartitionsDefinition(name=...)   # dynamic
```

### 4.3 assets/builder.py — 把 ①script + ②version 包成 asset

**[實作契約]** 這是 M2 的核心：把 flow owner 的 script 包進框架外殼，注入 config（承接 work_items 批次）、依賴、Pipes、版本、per-item 可觀測性。

```python
def build_asset(asset_spec, dimensions, version_fn, lsf_cfg, batching_cfg):
    partitions_def = build_partitions_def(asset_spec, dimensions)
    deps = build_deps(asset_spec, dimensions)        # 用 build_mapping
    script_fn = import_callable(asset_spec.script)

    if asset_spec.kind == "entry":
        @dg.asset(name=asset_spec.name)
        def _entry() -> dg.MaterializeResult:
            return dg.MaterializeResult()
        return _entry

    # compute:config 帶 work items;此 body 由 M4 LSFRunLauncher bsub 過來,
    # 在 LSF node 上以 in_process executor 跑;循序處理該批 items。
    # 【鐵則】命令內絕不可再 bsub(launcher 已做;這裡再 bsub = nested,§0 第 4 條)
    class _Config(dg.Config):
        items: list[str]

    # asset 級 op_tags:M4 LSFRunLauncher 從 run.tags["lsf/*"] 讀資源宣告
    # (M3 sensor 在 RunRequest 時刻 stamp;materialize CLI 走 op_tags)
    op_tags = {
        "lsf/queue":    lsf_cfg.queue,
        "lsf/cores":    str(lsf_cfg.cores),
        "lsf/mem_mb":   str(lsf_cfg.mem_mb),
        "lsf/walltime": lsf_cfg.walltime,
    }
    if lsf_cfg.project:
        op_tags["lsf/project"] = lsf_cfg.project

    # ⚠️ 實測雷(D1 驗證,2026-06-04):asset body 的 `context` 參數註解必須是
    # 裸名 `AssetExecutionContext`(`from dagster import AssetExecutionContext`),
    # 【不可】寫 `dg.AssetExecutionContext`,且該模組【不可】用
    # `from __future__ import annotations`。Dagster 1.13.3 會把 context 註解
    # 解析成型別來驗證;PEP-563 字串註解或限定名都會觸發
    # DagsterInvalidDefinitionError。見 spec_dagster/LESSONS.md L1。
    @dg.asset(
        name=asset_spec.name,
        partitions_def=partitions_def,
        deps=deps,
        op_tags=op_tags,
        code_version=...,                            # 連結到 version 策略
    )
    def _asset(context: AssetExecutionContext, config: _Config,
               pipes_subprocess_client: dg.PipesSubprocessClient,
               ) -> dg.MaterializeResult:
        coarse = context.partition_key
        for item in config.items:
            # Option 1:同節點 PipesSubprocessClient 收 EDA 工具 stdout +
            # 結構化 materialization;script 提供 local_command(無 bsub)
            pipes_subprocess_client.run(
                command=script_fn.local_command(coarse, item),
                context=context,
                extras={"item": item, "coarse_key": coarse},
            )
            # Option 2(若不需 collector):直接 script_fn(...) + log_event
            context.log_event(dg.AssetMaterialization(
                asset_key=f"{asset_spec.name}_item",
                partition=f"{coarse}|{item}",
                metadata={"data_version": version_fn(coarse, item)},
            ))
        return dg.MaterializeResult(metadata={"items_done": len(config.items)})
    return _asset
```

**[實作契約] generator kind 的契約(D1 補,2026-06-04)**:上面詳述了 compute
kind;`kind: generator`(輕量轉換,如 liberate-char 的 template_tcl/netlist…)
需要對稱的契約 —— **generator script 函數回傳 `dict[abs_path, content]`(純資料,
不 import dagster);framework 負責寫檔 + 對串接內容算 content_hash data_version。**
這樣 script 保持純粹,framework 獨佔持久化與版本。reference flow `netlist_files`
只有一個 compute、沒有 generator,所以原白皮書沒寫到這條;liberate-char 有 6 個
generator,逼出了這個缺口。見 `spec_dagster/flows/liberate_char/script.py`。

### 4.4 generator.py — 入口

```python
def build_definitions(flows_dir: str) -> dg.Definitions:
    specs = load_all_specs(flows_dir)            # 掃 flows/*/spec.yaml + 驗證
    assets, sensors, jobs = [], [], []
    for spec in specs:
        for a in spec.assets:
            version_fn = resolve_version(a.version)   # 基礎版或自訂
            asset = build_asset(a, spec.dimensions, version_fn,
                                resolve_lsf(spec, a), resolve_batching(spec, a))
            assets.append(asset)
            if a.work_items:                          # 需要 grouping sensor
                job = dg.define_asset_job(f"{a.name}_job", selection=[a.name])
                jobs.append(job)
                sensors.append(build_sensor(a, job, spec))   # M3
    return dg.Definitions(
        assets=assets, sensors=sensors, jobs=jobs,
        # M4 LSFRunLauncher 在 dagster.yaml 配置(§6.1),不是 Definitions 的 resource。
        # 此處 PipesSubprocessClient 只在 asset body 同節點收 EDA 工具輸出用(§4.3 Option 1)
        resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
        executor=dg.in_process_executor,             # load-bearing:run 在 LSF node 內順序跑該批
    )
```

---

## 5. M3 — Grouping Sensor（reconciliation）

### 5.1 planner.py — 純函數（必測）

```python
from collections import defaultdict

def plan_batches(
    desired: set[tuple[str, str]],     # {(coarse_key, item)}
    observed: set[tuple[str, str]],    # {(coarse_key, item)} 已完成
    batch_size: int,
) -> list[tuple[str, list[str]]]:
    """回傳 [(coarse_key, [items])]。純函數,不碰 Dagster/LSF/FS。"""
    todo = desired - observed
    buckets: dict[str, list[str]] = defaultdict(list)
    for coarse, item in todo:
        buckets[coarse].append(item)
    out = []
    for coarse, items in buckets.items():
        for i in range(0, len(sorted_items := sorted(items)), batch_size):
            out.append((coarse, sorted_items[i:i + batch_size]))
    return out
```

**[實作契約] test_planner.py：** 剛好 batch_size、尾數、跨 coarse_key 不混、空輸入、observed 已涵蓋則不發、排序穩定（同輸入兩次結果一致）。

### 5.2 reconcile.py — ReadinessSource（兩軌抽象）

```python
from typing import Protocol

class ReadinessSource(Protocol):
    def observed(self, instance, asset_key: str) -> set[tuple[str, str]]: ...

class EventLogSource:
    """從 Dagster materialization event 查已完成 items。"""
    def observed(self, instance, asset_key):
        # 查 <asset>_item 的 materialization,反解 partition "coarse|item"
        ...

class MarkerSource:
    """legacy:掃 NFS done-marker。"""
    def observed(self, instance, asset_key):
        ...
```

**[實作契約]** reference flow 用 `EventLogSource`。`MarkerSource` 先留介面 + `# FUTURE` stub，不實作（除非 reference 需要）。

### 5.3 factory.py — build_sensor

```python
def build_sensor(asset_spec, job, spec):
    source = EventLogSource()
    batch_size = resolve_batching(spec, asset_spec).size
    registry_key = spec.dimensions[asset_spec.work_items].source

    # default_status=RUNNING 是 D1 實測的關鍵(2026-06-04):air-gap 無 UI 可開
    # sensor,daemon 預設載入為 STOPPED 就什麼都不發。設 RUNNING 後
    # `dagster-daemon run` 首個 tick 就評估。見 spec_dagster/LESSONS.md L6。
    @dg.sensor(name=f"{asset_spec.name}_sensor", job=job,
               minimum_interval_seconds=60,
               default_status=dg.DefaultSensorStatus.RUNNING)
    def _sensor(context, **resources):
        registry = resources[registry_key]
        desired = registry.list_desired(asset_spec)       # set[(coarse, item)]
        observed = source.observed(context.instance, f"{asset_spec.name}_item")
        batches = plan_batches(desired, observed, batch_size)
        requests = [
            dg.RunRequest(
                run_key=_stable_key(coarse, items),        # hashlib!
                partition_key=coarse,
                run_config={"ops": {asset_spec.name: {"config": {"items": items}}}},
            )
            for coarse, items in batches
        ]
        return dg.SensorResult(run_requests=requests,
                               cursor=_fingerprint(desired))
    return _sensor

def _stable_key(coarse, items):
    import hashlib
    h = hashlib.md5("|".join(sorted(items)).encode()).hexdigest()[:12]
    return f"{coarse}:{h}"
```

**[實作契約]**
- run_key 用 `hashlib.md5`（跨 process 穩定）。**禁止 `hash()`**。
- cursor 只存 `_fingerprint(desired)`（一個 SHA256），不存 desired 集合本身。
- 狀態真實來源是 event log（observed），不是 cursor。
- **效能警告**：`source.observed()` 的 event log 查詢要批次化（一次撈，不要 per-item 查）。`registry.list_desired()` 不得在迴圈內做 I/O。sensor tick 應在數秒內完成；若超過，檢查這兩處的 I/O。

---

## 6. M4/M5 — Launch 層 + Run worker(LSF + Postgres backend)

### 6.1 M4 — dagster.yaml(兩份,DAGSTER_HOME 切換)

新模型下 dagster.yaml 是「跨層 backend 的接縫」：M4(orchestrator 上的 launcher)和
M5(LSF node 上的 run worker)只能靠**共享 storage** 對齊狀態。prod 必須 Postgres;
local-sim 沒遠端 worker 故 SQLite 即可。提供兩份模板,以**不同 `$DAGSTER_HOME` 目錄**
切換,不要在同一份 yaml 內塞 env-var 條件邏輯。

**prod — `framework/config/dagster.prod.yaml`**：

```yaml
# 跨主機共享 — 遠端 LSF worker 必須能寫回:三個 stanza 指向同一個 DB
storage:
  postgres:
    postgres_db:
      hostname: pg.internal
      username: dagster
      password: { env: DAGSTER_PG_PASSWORD }
      db_name: dagster
      port: 5432

compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /local/dagster_home/compute_logs

# M4:自寫 launcher,1 run = 1 bsub = 1 LSF job
run_launcher:
  module: framework.launcher.lsf_run_launcher
  class: LSFRunLauncher
  config:
    default_queue: normal
    default_cores: 4
    default_mem_mb: 4096
    default_walltime: "24:00"
    log_dir: /local/dagster_home/lsf_logs

run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    # 公式:max_concurrent_runs = min(
    #   LSF_user_slot_limit,                                  # busers / bqueues -l
    #   floor((PG_max_connections - reserved) / conns_per_worker)
    # )
    # - conns_per_worker ≈ 1-2(in_process worker 整個 lifetime 握一條 event-log writer 連線)
    # - reserved ≈ daemon(>=5) + webserver(>=5) + code servers
    # - 例:PG max_connections=200, reserved=40 → 160/2 = 80 worker ceiling
    # - 若 LSF 給此 user 500 slots,binding 端是 80(PG)
    # 從 64 起,觀察 pg_stat_activity + busers 後上調;
    # 若部 PgBouncer transaction pooling 則 PG 項放寬,改 LSF 端 binding
    max_concurrent_runs: 64
    dequeue_use_threads: true
    dequeue_num_workers: 8                       # 餵 launcher bsub 吞吐
    tag_concurrency_limits:
      - key: "eda/tool"
        value: "primetime"
        limit: 20                                # license-bound 家族上限(主機容量已交 LSF)

run_monitoring:
  enabled: true
  start_timeout_seconds: 1800                    # PEND 太久不誤殺(LSF queue 可能很長)
  cancel_timeout_seconds: 300
  max_runtime_seconds: 86400
  poll_interval_seconds: 120                     # 每 run / 每 2 分鐘 bjobs 一次健康檢查

telemetry:
  enabled: false
```

**local-sim — `framework/config/dagster.localsim.yaml`**(dev 與整合測試,無遠端 worker)：

```yaml
# storage 省略 → 預設 SQLite in $DAGSTER_HOME(單機可)
run_launcher:
  module: dagster._core.launcher
  class: DefaultRunLauncher                      # local subprocess;mock bsub on PATH 模擬 LSF
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 8                       # bounded by dev host,NOT LSF/PG
run_monitoring:
  enabled: true
telemetry:
  enabled: false
```

**[實作契約]**
- DAGSTER_HOME 仍**本機磁碟**(`/local/dagster_home/*`)、不放 NFS;prod 把 run/event/schedule
  store 搬到 Postgres,舊 SQLite-on-NFS 鎖問題隨之消失
- `DAGSTER_PG_PASSWORD` 走 env(`{ env: ... }` 語法);DB host 內網,無 internet egress
- 部署前驗證:LSF 計算節點 `nc -zv pg.internal 5432` 必須通(否則 worker 起來連不到 DB)
- 跑 `dagster instance migrate` 退 0(schema 一致);`psql -c "select count(*) from pg_stat_activity"` 確認連線在預算內

**[資源天花板:Postgres 連線是真正的 `max_concurrent_runs` bound]**

10k workers 每人至少握一條 event-log writer 連線;直接 10k 連線在 PG 不可能
(`max_connections` 預設 ~100-200)。所以真正的並行天花板 = `floor((PG_max_connections −
reserved) / conns_per_worker)`,**不是** LSF slots,也**不是**舊 host-fork 上限 50。
10k requests 仍會全部排隊(coordinator 排或 LSF PEND),`max_concurrent_runs` 只 gate
**STARTED**(in-flight)數。緩解(air-gapped 可用,排序):

1. **PgBouncer transaction pooling**(corpus 已認可,`personalities/dagster-expert/learn/12-scaling/POSTGRES_MIGRATION.md:221-222`)— 唯一能讓 STARTED 數逼近 LSF 容量的手段;air-gapped 自帶 binary
2. **`max_concurrent_runs` 上限**(最簡單正確的界)
3. **提高 PG `max_connections` + 調 `shared_buffers`/`work_mem`** — 有限 headroom,費 RAM
4. **粗化 batch size** 減少 run 數從而減少連線;per-item materialization + run_key-subset
   保留邏輯重試(§7),所以粗 batch 不傷重試粒度

**不**承諾「batched event writes」這種 1.13.3 沒有的旋鈕。

### 6.2 M4 — `LSFRunLauncher`(自寫,RunLauncher 子類)

繼承 `dagster._core.launcher.RunLauncher` + `dagster._serdes.ConfigurableClass`;
**`supports_check_run_worker_health = True`** 讓 `run_monitoring` daemon 走 bjobs 健康檢查。

```python
# framework/launcher/lsf_run_launcher.py
import os, re, subprocess
from dagster._core.launcher import (
    RunLauncher, LaunchRunContext, CheckRunHealthResult, WorkerStatus,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData

LSF_JOB_ID_TAG = "lsf/job_id"   # job_id 持久化在 run.tags;單一真相

class LSFRunLauncher(RunLauncher, ConfigurableClass):
    """1 Dagster run = 1 bsub = 1 LSF job.

    為何自寫:>10k 同時 run requests,orchestrator 無法 fork >10k 個 worker
    process。run worker(`dagster api execute_run`)在 LSF node 以 in_process
    executor 跑該批 items;回寫 run/event/schedule store 走內網 Postgres。
    """
    supports_check_run_worker_health = True

    def __init__(self, default_queue="normal", default_cores=4,
                 default_mem_mb=4096, default_walltime="24:00",
                 log_dir="/local/dagster_home/lsf_logs", project=None,
                 inst_data: ConfigurableClassData | None = None):
        self._default_queue   = default_queue
        self._default_cores   = default_cores
        self._default_mem_mb  = default_mem_mb
        self._default_walltime= default_walltime
        self._project         = project
        self._log_dir         = log_dir
        self._inst_data       = inst_data
        super().__init__()

    # ConfigurableClass 三件套
    @property
    def inst_data(self): return self._inst_data
    @classmethod
    def config_type(cls):
        from dagster import Field, IntSource, StringSource
        return {
            "default_queue":    Field(StringSource, is_required=False, default_value="normal"),
            "default_cores":    Field(IntSource,    is_required=False, default_value=4),
            "default_mem_mb":   Field(IntSource,    is_required=False, default_value=4096),
            "default_walltime": Field(StringSource, is_required=False, default_value="24:00"),
            "project":          Field(StringSource, is_required=False),
            "log_dir":          Field(StringSource, is_required=False,
                                      default_value="/local/dagster_home/lsf_logs"),
        }
    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    # ---- launch ----------------------------------------------------------
    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        # `dagster api execute_run <json>` 的 argv;1.13.3 確切 helper 名稱實作時
        # LIBRARIAN-consult(`/lookup-api LaunchRunContext`);
        # fallback: ExecuteRunArgs(pipeline_origin=run.job_code_origin,
        #     run_id=run.run_id, instance_ref=self._instance.get_ref()).get_command_args()
        args = context.run_worker_command

        # M2/M3 把 spec 的 lsf 區塊寫進 run.tags / op_tags(§4.3),launcher 端讀
        queue = run.tags.get("lsf/queue",    self._default_queue)
        cores = run.tags.get("lsf/cores",    str(self._default_cores))
        mem   = run.tags.get("lsf/mem_mb",   str(self._default_mem_mb))
        wall  = run.tags.get("lsf/walltime", self._default_walltime)
        proj  = run.tags.get("lsf/project",  self._project)

        os.makedirs(self._log_dir, exist_ok=True)
        out = f"{self._log_dir}/{run.run_id}.out"
        err = f"{self._log_dir}/{run.run_id}.err"

        bsub = [
            "bsub",
            "-J", f"dagster_run_{run.run_id[:8]}",
            "-q", queue, "-n", str(cores),
            "-R", f"rusage[mem={mem}]", "-W", wall,
            "-o", out, "-e", err,
            "-env", "DAGSTER_HOME,DAGSTER_PG_PASSWORD,PATH,PYTHONPATH",  # worker 連 PG 所需
        ]
        if proj:
            bsub += ["-P", proj]
        bsub += args                                          # async 投遞;不加 -K

        proc = subprocess.run(bsub, capture_output=True, text=True, check=True)
        m = re.search(r"Job <(\d+)> is submitted", proc.stdout)
        job_id = m.group(1) if m else ""

        # 持久化 job_id 到 run.tags(Postgres run store;daemon 重啟也不丟)
        self._instance.add_run_tags(run.run_id, {LSF_JOB_ID_TAG: job_id})
        self._instance.report_engine_event(
            f"Submitted to LSF as job {job_id} (queue={queue}, cores={cores})",
            run, cls=self.__class__,
        )

    # ---- terminate -------------------------------------------------------
    def terminate(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id)
        job_id = run.tags.get(LSF_JOB_ID_TAG) if run else None
        if not job_id:
            return False
        self._instance.report_run_canceling(run)
        subprocess.run(["bkill", job_id], check=False)
        return True

    # ---- check_run_worker_health(run_monitoring daemon 呼叫)-------------
    def check_run_worker_health(self, run) -> CheckRunHealthResult:
        job_id = run.tags.get(LSF_JOB_ID_TAG)
        if not job_id:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "no LSF job id on run tags")
        r = subprocess.run(
            ["bjobs", "-a", "-o", "stat exit_code", "-noheader", job_id],
            capture_output=True, text=True,
        )
        if r.returncode != 0 or not r.stdout.strip():
            # bjobs 對 DONE/EXIT 約 1 小時後遺忘;此時以 event log 終端事件為準
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "bjobs no record")
        state = r.stdout.split()[0]
        return {
            "PEND":  CheckRunHealthResult(WorkerStatus.RUNNING),
            "RUN":   CheckRunHealthResult(WorkerStatus.RUNNING),
            "DONE":  CheckRunHealthResult(WorkerStatus.SUCCESS),
            "EXIT":  CheckRunHealthResult(WorkerStatus.FAILED, "LSF job EXIT"),
            "PSUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
            "USUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
            "SSUSP": CheckRunHealthResult(WorkerStatus.RUNNING),
        }.get(state, CheckRunHealthResult(WorkerStatus.UNKNOWN, f"LSF stat={state}"))
```

worker 端 — flow owner 的 script 在 LSF node 上跑,asset body 內**同節點**
PipesSubprocessClient 收 EDA 工具輸出(`script.local_command(...)` 提供命令,絕不含 bsub):

```python
# flows/<name>/script.py
def local_command(coarse, item):
    return ["liberate", "-cell", item, "-corner", coarse]   # 絕不含 bsub

# 若選擇 dagster_pipes 內部呼叫(§4.3 Option 1 inner script):
from dagster_pipes import open_dagster_pipes
with open_dagster_pipes() as pipes:                          # 同節點 file-based
    item   = pipes.extras["item"]
    coarse = pipes.extras["coarse_key"]
    run_eda_tool(coarse, item)
    pipes.report_asset_materialization(
        asset_key=f"netlist_files_item",
        metadata={"item": item},
        partition=f"{coarse}|{item}",
    )
```

**[實作契約]**
- 一個 run = 一個 bsub;asset body 內絕不再 bsub(否則 nested,§0 第 4 條 + 附錄 A 第 7 條)
- `_bsub` / `_bjobs_state` / `_bkill` 抽到 `framework/launcher/bsub.py`,獨立 pytest(fake subprocess)
- `LaunchRunContext.run_worker_command` 是否 1.13.3 helper 屬性,實作時 LIBRARIAN-consult
  (`/lookup-api LaunchRunContext`);若無就用 `ExecuteRunArgs(...).get_command_args()`
- `add_run_tags("lsf/job_id", ...)` 是 terminate / health-check 的單一真相;daemon 重啟也不丟
  (Postgres run store)
- worker 端 PipesSubprocessClient 走**同節點 temp file**(非 NFS,非 S3/GCS/Azure)— 跨主機
  狀態同步交給 Postgres,不再經 file Pipes
- **D1 實證(C 階段 mock-bsub 整合測試)**:在正常 Dagster 配線之外(如單元測試手建 launcher),
  必須用 `launcher.register_instance(instance)` 掛載 — 直接賦值 `_instance` 會
  AttributeError(它是 RunLauncher base class 的 read-only property)。同樣,需要 stand-in
  `DagsterRun` 時用 `@dg.job` decorator 建 noop job 後 `instance.create_run_for_job(...)`;
  `dg.define_asset_job(...)` 回的是 `UnresolvedAssetJobDefinition`,`create_run_for_job` 會拒。
  見 `spec_dagster/tests/test_lsf_launcher.py` 與 LESSONS.md L13/L14。

### 6.3 資源與效能分析(四個負載面)

新模型把成本從 orchestrator 卸到 LSF + Postgres。逐面看天花板與調節旋鈕：

**(1) Orchestrator host — 大幅減負(反轉的主要動機)**
- 舊:DefaultRunLauncher fork 一個 Python run worker process / run;>10k 即不可能
- 新:launcher 只組 flag + `subprocess.run(["bsub", ...])`,每 run 成本 ≈ 一次 bsub
  (數十-數百 ms)+ 一次 `add_run_tags`。orchestrator 上**零** worker process
- 新 bound = **dequeue throughput** ≈ `dequeue_num_workers / bsub latency`。
  `dequeue_use_threads: true` + `dequeue_num_workers: 8`(§6.1)讓 bsub 並行
- 健康檢查負載:1 bjobs / STARTED run / 120s;ceiling 64 → ~0.5 bjobs/s,trivial。
  未來 ceiling 上千時改用一次 `bjobs -u $USER` 批撈
- 注意:有些 LSF site 對 `bsub` 設 rate limit(每秒 N 次),觸到要與 LSF admin 確認

**(2) LSF grid — batch size 是主旋鈕**
- 同時 in-flight 的 bsub 數 = `max_concurrent_runs`(1 run = 1 bsub)
- 一個 wave 的 LSF job 總數 = `ceil(Σ todo_items / batch_size)` per coarse key
- 大 batch(例 500):LSF scheduling/fair-share 負載低、PG 連線少、bsub 少;但 EXIT 損失大、wall-clock 長(順序)、straggler 佔 slot 久
- 小 batch(例 20):細粒度重試、wall-clock 短、負載分散;但 25× LSF job + bsub + 連線
- **關鍵**:邏輯重試粒度與 batch size **無關** — §7 的 per-item materialization +
  reconciliation `desired − observed` + run_key-subset 保留 fine-grained retry;粗 batch
  ≠ 粗重試。**預設 batch 100**(對齊 §3),只有 LSF overhead 或 straggler 嚴重才調

**(3) Postgres — 真正的 `max_concurrent_runs` 上限(§6.1 已詳述)**
- 10k workers 同時連 PG 不可能;`floor((max_connections − reserved) / conns_per_worker)` 為界
- 4 個緩解見 §6.1:PgBouncer(最高槓桿)/ ceiling cap / 提高 PG max_connections / 粗 batch
- 事件寫競爭:daemon + webserver + N workers 寫同一個 event_logs 表 → PG 與 orchestrator
  co-locate;`dagster instance migrate` 確保索引;PgBouncer 降低連線爭用

**(4) Coordinator — 直到 ceiling 上千才會是瓶頸**
- 餵 STARTED 數的需求 = `dequeue_num_workers / bsub_latency ≥ ceiling / 平均 run 秒數`
- 例:ceiling=64,T=600s → 需 ~0.1 launch/s,trivially 達到
- `tag_concurrency_limits` (例 `eda/tool: primetime: 20`)現在是 **license/tool 家族上限**,
  與主機容量無關(主機容量 binding 已被 LSF 取代)

**保守起始公式**：

```
max_concurrent_runs = min(
    LSF_user_slot_limit,                                       # busers / bqueues -l
    floor((PG_max_connections - reserved) / conns_per_worker)  # 通常 binding
)
# 預設 64;真實負載下觀察 pg_stat_activity + busers 後上調
# 部 PgBouncer 後 PG 項放寬,改 LSF 端 binding
```

---

## 7. 中斷與恢復（必須驗證的行為）

reconciliation + per-item 記錄的組合，保證任何中斷後自動收斂。**[實作契約] 這些行為要寫成整合測試或手動驗證腳本：**

機制換、行為不變:LSF job 死亡的偵測現在由 **launcher `check_run_worker_health` +
`run_monitoring` daemon**(§6.2)觸發 — daemon 每 `poll_interval_seconds`(預設 120s)
bjobs 一次,EXIT 即標 run FAILED,sensor 下次 tick 走 reconciliation 重發。asset body
**不再**主動輪詢 bjobs。

| 中斷點 | 預期行為 | 驗證方法 |
|---|---|---|
| sensor tick 中途中斷 | 下次 tick 重算 desired−observed，未做的重新發 | 殺掉 daemon，重啟，確認剩餘 batch 被撿起 |
| LSF job EXIT | `check_run_worker_health` 偵測 → run 標 FAILED → 下次 tick reconciliation 重發 | mock 一個 EXIT bjobs 回應,確認 run_monitoring 在 1-2 個 poll 週期內標失敗、下次 tick 重發 |
| LSF job PEND 太久 | `run_monitoring.start_timeout_seconds: 1800`(§6.1)給足排隊時間,不誤殺 | 模擬持續 PEND 30 分鐘,確認 run 未被誤判 |
| batch 部分成功（100 做了 60） | 60 個 item 的 materialization 留存(Postgres event log),下次只補 40 | 中途 kill,確認下次 desired−observed = 40 |
| daemon 重啟 | 狀態在 Postgres,續跑;`lsf/job_id` 在 run.tags 仍可用於 terminate/health-check | 重啟 daemon,確認不遺失、不重做、health-check 仍能撈到 job |

**[實作契約] run_key 與重試**：reference flow 用「未完成 items 子集 → run_key 隨之變化 → 可重發」。即失敗後剩下的 items 不同，hashlib 算出的 run_key 不同，不會被去重擋掉。長 job 才考慮 Dagster `RetryPolicy`（reference 不需要）。

---

## 8. 用既有 case 建立測試（reference flow）

### 8.1 reference flow 規格

用你現有的 `netlist_files` 案例作為第一個 reference：

```
flows/netlist/
  spec.yaml          # §3.1 那份
  script.py          # 包含 run_netlist + bsub_command；可先用假 EDA 工具(echo/sleep)
  tests/test_script.py
```

**[實作契約] script.py 的 reference 實作**先用「假運算」（`sleep` + 寫一個假產物檔），讓端到端能跑通而不依賴真 EDA 工具。真工具接入是 flow owner 後續的事。

### 8.2 測試金字塔

```
單元測試（純函數,不碰 Dagster/LSF/FS） — 最多、最快
  test_mapping_builder.py    方向正確性（§4.1)
  test_planner.py            分批邏輯（§5.1)
  test_versioning.py         三種基礎版 + 自訂 interface
  test_spec_schema.py        spec 驗證
  test_bsub.py               bsub / bjobs / bkill 組裝(fake subprocess;framework/launcher/bsub.py)

整合測試（用 Dagster 但不碰真 LSF）
  test_generator.py          build_definitions(reference spec) 成功產出 assets/sensors
  test_sensor_integration.py 用 DagsterInstance.ephemeral() + fake registry,
                             驗證 sensor 發出正確數量的 RunRequest
  test_asset_inprocess       asset body 純 in-process(無 bsub),emit per-item materialization
  test_asset_samenode_pipes  (選用)Option 1 路徑:同節點 PipesSubprocessClient 收 EDA 工具 stdout
  test_launcher_with_mock_bsub
                             mock bsub/bjobs/bkill on PATH,驗 launch_run / terminate /
                             check_run_worker_health 三路徑;assert job_id 寫進 run.tags;
                             bjobs 狀態(DONE/EXIT/PEND/RUN)正確映射 WorkerStatus

端到端（手動或 CI,本機模擬)
  local-sim 模式(dagster.localsim.yaml + DefaultRunLauncher + mock bsub on PATH),
  跑完整 reference flow:start → sensor → batch → 假運算 → materialization
  驗證中斷恢復(§7);prod 模式需要真 Postgres + (選)PgBouncer + 真 LSF 才能完整測
```

**[實作契約]** 先讓單元測試全綠，再做整合，最後端到端。每個里程碑（§9）對應一層測試。

### 8.3 驗證 reference flow 跑通的判準

```
□ build_definitions("flows/") 載入無錯,UI 顯示 start + netlist_files
□ partition 只有 trio_group(g0/g1/g2),cell 不在 partition(降維成功)
□ materialize start 後,netlist_sensor 偵測到、發出 RunRequest
□ 1800 假 cell → 18 個 run(每 100),run_key 各異不撞
□ 每個 run 透過 Pipes(本機模擬)跑假運算,回報 per-cell materialization
□ 中途 kill daemon,重啟後只補未完成的(reconciliation 生效)
□ sensor tick 在數秒內完成(無 §5.3 的效能問題)
```

---

## 9. 實作里程碑（交付順序）

每個里程碑結束都要能 demo + 測試綠。

**M0 — 骨架（0.5 天）**
- 專案結構（§2）、`pyproject.toml`、`pip install -e .`、DAGSTER_HOME 設定。
- 空的 `build_definitions` 回傳空 `Definitions`，`dagster dev` 能起。

**M1 — Spec schema（0.5 天）**
- §3 的 Pydantic schema + loader + 驗證規則。
- `test_spec_schema.py` 綠。能載入 reference spec.yaml 並驗證。

**M2 — 產生器核心（2 天）**
- mapping_builder（§4.1）+ partition_builder（§4.2）+ asset builder（§4.3）+ generator（§4.4）。
- `test_mapping_builder.py`、`test_generator.py` 綠。
- 判準：`build_definitions` 能從 reference spec 產出 start + netlist_files，UI 顯示，partition 降維正確。

**M3 — Grouping sensor（1.5 天）**
- planner（§5.1）+ reconcile/EventLogSource（§5.2）+ factory（§5.3）。
- `test_planner.py`、`test_sensor_integration.py` 綠。
- 判準：materialize start 後 sensor 發出正確的 18 個 RunRequest。

**M4/M5 — Launch + Run worker + Postgres backend（4 天）**
- 兩份 dagster.yaml（§6.1 prod + localsim）+ LSFRunLauncher（§6.2）+ `framework/launcher/bsub.py` + versioning 基礎版。
- Postgres bring-up + `dagster instance migrate` + (選)PgBouncer + 連線天花板量測。
- mock bsub/bjobs/bkill shims(local-sim 用,non-LSF host 可跑)。
- `test_bsub.py`、`test_versioning.py`、`test_launcher_with_mock_bsub`、`test_asset_inprocess` 綠。
- 判準:local-sim 模式跑通 reference flow;prod 模式 launcher 對 mock bsub 行為等價於對真 bsub。

**M6 — 中斷恢復 + 收尾（1.5 天）**
- §7 的中斷恢復驗證(含 LSF EXIT → run_monitoring 偵測 → reconciliation 重發路徑)。`_template/` 範本。README(含 §3.5 onboarding SOP 補充說明)。
- 判準：§8.3 的所有 checkbox 通過。

**總計約 9.5 天。** Postgres + (選)PgBouncer 基礎設施**屬前置**,不計入此 9.5 天。
真 LSF 接入(換掉 mock bsub)是 M6 之後的獨立階段,因為它需要在真叢集上測;launcher
程式碼本身在 mock bsub 下即可完整驗證(launch / terminate / health-check 三條路徑)。

---

## 10. 給實作 agent 的驗收清單

實作完成時，逐項確認：

```
架構正確性
□ framework/ 不 import flows/ 任何具體 flow
□ flow owner 只需提供 script + (可選)data_version + spec.yaml
□ M2/M3 從 spec 程式化產生,無逐 flow hardcode
□ M4/M5 是共用元件,spec 只餵參數

關鍵契約
□ mapping 方向由 build_mapping 封裝,有單元測試驗證 dict key = upstream
□ flow owner 程式碼中沒有任何 MultiPartitionMapping 手寫
□ sensor 是 reconciliation(desired−observed),cursor 只存指紋
□ run_key 用 hashlib,全 codebase 無 hash() 用於 run_key
□ LSF 走 Pipes,無自寫 LSFRunLauncher,無 nested bsub
□ PipesFileContextInjector/Reader(非 S3/GCS/Azure)
□ 中斷時 bkill,不留 orphan

測試
□ 所有純函數有獨立 pytest,不碰 Dagster/LSF/FS
□ test_mapping_builder 含 identity+all_of 混用案例
□ test_planner 含尾數、跨 coarse 不混、排序穩定
□ reference flow 端到端跑通(§8.3 全綠)
□ 中斷恢復四情境驗證(§7)

環境
□ DAGSTER_HOME 在本機磁碟
□ pip install -e . 可裝,daemon/webserver 同環境能 import
□ dagster-pipes 可 vendor 進外部 worker 環境
```

---

## 附錄 A — 關鍵防錯備忘（來自踩坑經驗）

實作 agent 特別注意這些已知坑：

1. **MultiPartitionMapping 方向**：dict key = upstream，dimension_name = downstream。對照官方範例 weekly_abc→daily_123：`{"abc": DimensionPartitionMapping(dimension_name="123", ...)}`。
   - **單維上游 → 多維下游(D1 實證)**:當上游只分一個維度、下游是 `MultiPartitionsDefinition`(如 liberate-char 的 `template_tcl[pvt] → characterize[pvt,cell]`),正確 primitive 是 `MultiToSingleDimensionPartitionMapping(partition_dimension_name=<共享維度>)`,**不是** `MultiPartitionMapping`。此類別在 1.13.3 是 **beta**(建構時噴 `BetaWarning`);行為正確,但需 pin 版本、必要時在 framework 邊界 suppress。見 `spec_dagster/framework/assets/mapping_builder.py` + `LESSONS.md` L2。
2. **StaticPartitionMapping 只能用於兩端都 static** 的維度。dynamic 維度（如 cell）用會報 `can only be defined between two StaticPartitionsDefinitions`。所以 cell 維度走 all_of（AllPartitionMapping），不走 static。
3. **MultiPartitionsDefinition 只能有一個 dynamic 維度**。reference 的 cell 是降維成 config，不在 partition，所以不觸發此限制；但若未來把 cell 放回 partition 要注意。
4. **run_key 用 hash() 會因 PYTHONHASHSEED 跨 process 飄移**，破壞去重。一律 hashlib。
5. **sensor tick 慢**：通常不是 plan_batches（純運算快），而是 (a) event log 查詢沒批次化，或 (b) daemon 對 RunRequest 的 partition 驗證（dynamic partition 查詢）。reference 用 static trio_group 可避開 (b)。
6. **DAGSTER_HOME / NFS / SQLite**:DAGSTER_HOME 仍**錨在 orchestrator 本機磁碟**;但 prod 把 run/event/schedule store 搬到 Postgres(遠端 LSF worker 必須共享狀態)→ 舊 SQLite-on-NFS 鎖問題(`alembic exists`、tick 慢)隨之消失。local-sim 才用 SQLite,DAGSTER_HOME 仍本機。
7. **nested bsub(已反轉)**:**唯一 bsub 在 launcher**(`LSFRunLauncher`,1 run = 1 bsub)。asset body 內若再 bsub 就是 nested bsub(雙佔 LSF slot)。大規模(>~thousands of runs)用自寫 launcher(§6.2);小規模(< hundreds)才用 DefaultRunLauncher + asset 內 Pipes bsub(`personalities/dagster-expert/learn/13-lsf-integration/` Part A)。

## 附錄 B — Data Version 基礎版（versioning/base.py）

```python
import hashlib, time
from typing import Protocol, Callable

class VersionFn(Protocol):
    def __call__(self, coarse_key: str, item: str) -> str: ...

def timestamp_version(coarse_key, item) -> str:
    return str(time.time_ns())                         # 每次都新 → 總是重算

def content_hash_version(output_path_resolver: Callable) -> VersionFn:
    def _fn(coarse_key, item):
        path = output_path_resolver(coarse_key, item)
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:16]
    return _fn

def input_fingerprint_version(tool_version: str, input_resolver: Callable) -> VersionFn:
    def _fn(coarse_key, item):
        inputs = input_resolver(coarse_key, item)
        blob = f"{tool_version}|{inputs}".encode()
        return hashlib.sha256(blob).hexdigest()[:16]
    return _fn

# resolve_version("timestamp") → timestamp_version
# resolve_version("content_hash") → content_hash_version(...)
# resolve_version("flows.netlist.data_version:my_fn") → import 自訂
```

**[實作契約]** 三種基礎版 + 自訂 interface（VersionFn protocol）。`test_versioning.py` 驗證：timestamp 每次不同、content_hash 同內容相同、自訂 fn 可被 resolve_version 載入。

---

## 附錄 C — D2 application 等價性驗收的 5 個行為面向

> **角色**：D2(在 `spec_dagster` framework 上實作 liberate-char application)的
> 驗收清單。framework-generated 的 Dagster 物件,必須在以下 5 個面向上**行為**
> 等價於既有的 hand-rolled `personalities/flow-cartographer/examples/liberate-char/converted/`。
> 「結構性差異」（自動產出 vs 手寫的寫法不同)可接受;「行為性差異」(stale 不
> 同步、重跑沒回到原態、partition status 不一致…)不可接受。
>
> 蒸餾自 v1 `flow-cartographer/conversion-coverage/01..05`(2026-06-04 廢棄)。
> 原 v1 6×C 細部準則的精神,已被本附錄收斂為「面向 → 行為等價點 → 驗收方法」。
>
> **2026-06-05 D2 實證版**:每個面向的「驗收方法」已從概念描述換成具體
> API 呼叫與資料 receipt;末尾附 D2 實跑數據(全 PASS;見
> `spec_dagster/flows/liberate_char/EQUIVALENCE.md` 完整 raw artifact)。
> 弱 agent 走 D2 Phase C4 時,逐面向跑這份清單即可。

### C1 State management

| 必須等價的具體點 | 驗收方法(D2 實證) |
|---|---|
| Materialization records 每個 partition 有一筆 | `instance.get_materialized_partitions(AssetKey("<asset>"))` 兩端 set 相等 |
| `dagster/data_version` 計算結果 | 對每個已物化 partition:`get_event_records(EventRecordsFilter(event_type=DagsterEventType.ASSET_MATERIALIZATION, asset_key=..., asset_partitions=[pk]), limit=1, ascending=False)` 取最新 record;讀 `materialization.tags["dagster/data_version"]`(LESSONS.md L9/L10/L11);兩端每個 partition 的值必須相同 |
| 上游 data version chain 一致 | 同一 record 的 `dagster/input_data_version/<upstream>` 對每個 path-free 上游兩端必須相同(path-bearing 上游 — 內容含絕對路徑者 — 容許不同,屬已知結構差異;見 LESSONS.md L12) |
| Partition key 序列化形式一致 | `MultiPartitionsDefinition({"pvt": ..., "cell": ...})` 兩端序列化為 `cell|pvt` 字典序(`INV|tt_25` 而非 `tt_25|INV`) |
| 跨 process / 重啟後狀態仍然一致 | 重啟 daemon,`get_materialized_partitions` 與 data_version tag 不變(prod Postgres 天然滿足;local-sim 同 DAGSTER_HOME 即可) |

**Receipt(D2,liberate-char,9 leaves)**:
- 兩端 `len(get_materialized_partitions)` = **9/9 相等**
- 兩端每葉 `dagster/data_version` = **9/9 MATCH**(mock liberate 對 path-free content 算 SHA256,兩端輸入相同 → 輸出相同)
- 4 個 path-free 上游(`template_tcl`、`section_tcl`、`model_card`、`netlist`)的 `input_data_version` 鏈 = **9/9 MATCH**;2 個 path-bearing 上游(`cell_list`、`main_tcl`)依設計兩端不同(已說明於 LESSONS.md L12 與 EQUIVALENCE.md C1 結構差異欄)

### C2 Stop & rerun

| 必須等價的具體點 | 驗收方法(D2 實證) |
|---|---|
| 單一 partition rerun 只重跑該 leaf | `dg.materialize([asset], partition_key=MultiPartitionKey({...}))` 觸發後,新增 ASSET_MATERIALIZATION events 數量 = 1,且新增 record 的 `materialization.partition` 必須是該 leaf;不可觸到其它 partition |
| Run cancellation:**dispatch:local** 不留 orphan | `dagster run terminate <id>` → asset body 內的 PipesSubprocessClient 子程序自然死 |
| Run cancellation:**dispatch:lsf** bkill 到 LSF job | `LSFRunLauncher.terminate(run_id)` 從 `run.tags["lsf/job_id"]` 取 id 後 `bkill`;見 `framework/launcher/lsf_run_launcher.py` 與 `tests/test_lsf_launcher.py::test_terminate_bkills_persisted_job_id` |
| Batch 部分成功 → 下次 reconciliation 只補未完成的 | reconcile sensor `plan_reconcile(desired, observed)` 純函數;見 `framework/sensor/planner.py` 與 `tests/test_planner.py::test_partial_observed`;daemon 端日誌應出現「`<asset>: K missing of N -> K RunRequests`」並對應已物化集合更新 |
| `run_monitoring` 偵測 stuck run(prod LSF) | `LSFRunLauncher.supports_check_run_worker_health = True` + `check_run_worker_health` 從 bjobs 對映 WorkerStatus;`PEND/RUN -> RUNNING`、`DONE -> SUCCESS`、`EXIT -> FAILED`;見 `tests/test_lsf_launcher.py::test_map_state` 與 `test_check_run_worker_health_maps_*` |

**Receipt(D2,liberate-char `INV|tt_25` rerun)**:兩端 `rerun_new_materialization_count` = **1**;`rerun_touched_partitions` = `['INV|tt_25']`。框架版 LSF 路徑由 15 個 launcher 整合測試覆蓋,本機 mock bsub/bjobs/bkill on PATH 跑通。

### C3 Job scheduling

> 本面向 D2 允許**結構差異**:framework 用 reconcile sensor
> (`desired − observed`,`default_status=DefaultSensorStatus.RUNNING`),
> hand-rolled 用 `AutomationCondition.eager()` + `netlist_drop_sensor`
> (事件驅動)。兩個都是正確設計(LESSONS.md L6)。**行為**等價的證據在
> 「daemon 真的 emit RunRequests 然後 succeed」。

| 必須等價的具體點 | 驗收方法(D2 實證) |
|---|---|
| Daemon liveness(headless air-gap) | 啟動後 `daemon.log` 含「`Instance is configured with the following daemons: ['AssetDaemon', ..., 'SensorDaemon']`」 |
| Sensor evaluation 真的發生 | `daemon.log` 含「`Checking for new runs for sensor: <reconcile_sensor_name>`」 |
| Sensor 對 N 個 missing partition 發 N 個 RunRequest | `daemon.log` 含「`<asset>: K missing of N -> K RunRequests`」(framework 的 `build_sensor` 把這行寫入 context.log;見 `framework/sensor/factory.py`) |
| `tag_concurrency_limits` 對家族並行設限 | Spec 的 `op_tags["dagster/concurrency_key"]` + dagster.yaml 的 `tag_concurrency_limits` 兩端值一致 → `QueuedRunCoordinator` 分波洩流,觀察 `get_materialized_partitions` 隨時間單調遞增 |
| `default_status=RUNNING` 讓 headless daemon 自動啟用 sensor | framework `build_sensor` 已設(`framework/sensor/factory.py`);否則 daemon 載入為 STOPPED 不評估,踩 LESSONS.md L6 |

**Receipt(D2,`scripts/run_demo.py`)**:`characterize_reconcile_sensor` 第 1 個 tick log「`characterize: 9 missing of 9 -> 9 RunRequests`」,daemon launch 9 runs;`tag_concurrency_limits: liberate_run: 4` 下分波 **0 → 3 → 7 → 9**;23 runs 全 SUCCESS;9/9 leaf 物化。

### C4 Dependency definition

| 必須等價的具體點 | 驗收方法(D2 實證) |
|---|---|
| 唯一輸出風格 `deps=[AssetDep(...)]`(不用 `ins=`) | framework 端不會強制 IO load:`grep -rE "ins=" framework/assets/builder.py` 必須 0 命中 |
| Asset parent set 一致 | `ag = defs.resolve_asset_graph(); ag.get(AssetKey("<compute>")).parent_keys` 兩端 set 相等(注意:1.13.3 是 `resolve_asset_graph`,不是 `get_asset_graph`;見 LESSONS.md L3) |
| Partition keys 集合一致 | `node.partitions_def.get_partition_keys()` 兩端 set 相等 |
| 單維上游 → 多維下游用同一 primitive | 兩端都用 `MultiToSingleDimensionPartitionMapping(partition_dimension_name=<shared>)`;此 primitive 在 1.13.3 是 **beta**(LESSONS.md L2)— 行為正確,但會噴 `BetaWarning`,framework 邊界可 suppress |
| 不出現 `class .* PartitionMapping` subclass | `grep -rE "class .* *PartitionMapping" .` 全 repo 0 命中(framework 內 built-in 組合除外) |

**Receipt(D2,liberate-char)**:`characterize.parent_keys = {cell_list, main_tcl, model_card, netlist, section_tcl, template_tcl}` 兩端 set 相等(6/6);`MultiPartitionsDefinition({pvt, cell})` 9 partition keys 兩端集合一致;`MultiToSingleDimensionPartitionMapping` 為兩端共同 primitive。

### C5 Logs & env status

| 必須等價的具體點 | 驗收方法(D2 實證) |
|---|---|
| Pipes message channel 工作 | 兩端 `materialization.tags["dagster/data_version"]` 必須非空且相同 — 因為這條 tag 就是經 `pipes.report_asset_materialization(data_version=...)` 傳回來的,等同證明 channel OK(`PipesFileContextInjector`/`PipesFileMessageReader` 同節點 temp 路徑可達) |
| 結構化事件 ASSET_MATERIALIZATION 數量一致 | `EventRecordsFilter(event_type=DagsterEventType.ASSET_MATERIALIZATION, asset_key=...)`(LESSONS.md L10:`event_type` positional required)兩端 count 相等 |
| `dagster instance migrate`(schema 一致) | `instance.upgrade()` 退 0 — 兩端 SQLite 場景天然 OK;prod Postgres 場景必須 |
| Storage 配置 | **prod**:Postgres + `psql -c "select count(*) from pg_stat_activity where datname='dagster'"` 連線數在 §6.1 公式預算內。**local-sim**:SQLite + DAGSTER_HOME 本機磁碟(非 NFS;`alembic exists` 不應出現) |
| `dispatch: lsf` 下 worker env 通到 Postgres | bsub `-env` 列含 `DAGSTER_HOME,DAGSTER_PG_PASSWORD,PATH,PYTHONPATH`(LSFRunLauncher 已寫死);見 `tests/test_lsf_launcher.py::test_bsub_argv_env_forwarding_for_postgres` |

**Receipt(D2,liberate-char)**:`instance.upgrade()` 兩端綠;9/9 `dagster/data_version` 非空且兩端 MATCH(即 Pipes 確實雙向通);9/9 path-free `.ldb` digest MATCH(mock liberate 對 content 算的 SHA256);LSF env-forwarding 由 `test_bsub_argv_env_forwarding_for_postgres` 整合測試覆蓋。

### 驗收彙整模板(D2 已填好版;放在每個 application 的 `EQUIVALENCE.md`)

```
| 面向 | 結構差異(列舉,接受) | 行為差異(必須 0) | 結論 |
|---|---|---|---|
| C1 State            | path-bearing 上游 data_version 兩端不同(cell_list/main_tcl 內嵌 SOURCES root);framework 多檔 generator 把全部 content concat 一起 hash | 無 | PASS |
| C2 Stop & rerun     | (LSF 路徑由 mock bsub/bjobs/bkill 整合測試覆蓋,非生產 LSF) | 無 | PASS |
| C3 Job scheduling   | framework: reconcile sensor + `default_status=RUNNING`;hand-rolled: `AutomationCondition.eager()` + `netlist_drop_sensor` | 無 | PASS |
| C4 Dependency def   | (兩端結構完全相同) | 無 | PASS |
| C5 Logs & env       | (兩端結構完全相同) | 無 | PASS |
```

5 個面向全 PASS = D2 完成判準之一。實證見
`spec_dagster/flows/liberate_char/EQUIVALENCE.md`(自動 generated;`python -m
scripts.equivalence` 可重現)。

---

## 附錄 D — 已折疊 / 廢棄素材的去向(2026-06-04 personality 清理)

`personalities/flow-cartographer/` 下大部分檔案(v1 ralph-loop personality 機制)
於 2026-06-04 廢棄,僅保留:本白皮書、`D2_IMPLEMENTATION_PLAN.md`、`examples/liberate-char/`。
被廢素材的處置如下,以防未來查找。

| 原位置 (v1) | 處置 / 去向 |
|---|---|
| `conversion-coverage/01..05` + README | 蒸餾為本白皮書「附錄 C」5 面向驗收清單(行為等價,不再是 PASS/REJECT 審計) |
| `standards/tdd-rules.md` | TDD 已內化於白皮書 §0.6(「每個模組先寫測試再寫實作」)+ §8.2 測試金字塔 |
| `standards/clean-code-rules.md` | 由 framework 強制(spec 是 menu / Pydantic 驗證 / mapping_builder 封裝方向 / framework 不 import flows),不再需 personality 級 prose 規範 |
| `standards/refusal-patterns.md` | 由 Pydantic schema 載入時報錯取代(menu 不在範圍 → 載入失敗),不再需 prose refusal 模板 |
| `smoke/cli-conformance.md` + `smoke/graphql-conformance.md` | Dagster 1.13.3 CLI / GraphQL 覆蓋已在 `personalities/dagster-expert/database/dagster-1.13.3/docs/`;framework 測試直接 import dagster public API,以 install 為準 |
| `skills/{wake,plan-loop,build-loop,verify-loop,reflect-loop}/` | v1 ralph-loop SOP 廢棄。D1/D2 進度由 `D2_IMPLEMENTATION_PLAN.md` 階段表與 git commit 表示,不再 tick-driven |
| `scheduled/{plan,build,verify,reflect}.md` | 對應的 schedule 任務廢棄;`spec_dagster` framework 完成後若需要常駐 sensor,改在 framework 內以 spec `trigger:` 欄位宣告 |
| `flow-model/_plan.yaml` etc | v1 conversion-ledger state machine 廢棄;D1/D2 不用 ledger |
| `memory/` scaffolding(空 .gitkeep)| personality 暫無 SMAK 副本需要;未來真要做 D3(內部 agent migration kit)再建 |
| `ROLE.md` / `STATUS.md` / `TICK_GUIDE.md` / `PRE_FLIGHT_CHECKLIST.md` / `CONVERSION.md` / `QUICKSTART.{en,zh}.md` / `manifest.yaml` | v1 personality 機制廢棄。flow-cartographer 仍以 personality 形式存在(根 `AGENTS.md`、`MEMORY.md`、`README.md` 仍引用),但**內容**換成本白皮書 + D2 計畫;形式上的 personality scaffolding 不再維護 |
| `derived_from: dagster-ap-auditor`(v1 manifest 譜系)| 譜系記錄保留於本附錄;auditor PASS/REJECT 模式無 ongoing relevance |
| 弱-agent 設計心法(原於 v1 ROLE / standards) | 已在 root `MEMORY.md` L1「Lessons learned — designing personalities for less-capable agents」(機械式 trigger、pre-flight、tcsh-first、絕對路徑、verify-after-each-step、refusal-as-feature、visible state checkpoints) |

**Ripple 提醒**(本清理刻意不動):根 `AGENTS.md`(7 處 mention)、`README.md`
(6 處)、`MEMORY.md`(1 處)仍以「v1 ralph-loop personality」描述 flow-cartographer。
這些描述現已 stale;在 D1/D2 完成、`spec_dagster` framework 落地後,可一併修訂為
「framework 工作區 + 兩份核心文件」說法。
