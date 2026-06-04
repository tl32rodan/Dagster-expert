# 五層通用 Dagster 框架 — 實作白皮書

> **本文件的用途**：交給實作 agent（Kimi 2.5T）作為建構規格。
> **實作對象**：一個 spec-driven 的五層 Dagster 框架，flow owner 只提供 script + data version + YAML spec，框架自動產生 assets / partitions / mappings / sensors，並把運算投遞到 LSF。
> **環境**：air-gapped、CentOS 7、LSF、tcsh、NFS、Dagster 1.13.x（asset-centric）。
> **驗證方式**：用既有的 `netlist_files`（trio_group × cell 降維案例）作為第一個 reference flow，跑通端到端。

---

## 0. 給實作 agent 的總指示（先讀這段）

你要建構的不是一個 pipeline，而是一個**框架**：它讀 YAML spec，程式化（非 hardcode、非逐 flow 手寫）產生 Dagster 物件。實作時請嚴格遵守以下原則，違反任一條都算實作失敗：

1. **分層職責不可混**。五層各有單一職責（見 §1）。M2/M3 是「讀 spec 生成」，M4/M5 是「框架現成元件 + spec 餵參數」。
2. **flow owner 永遠不手寫 partition mapping**。mapping 由 §4 的 `build_mapping()` 從 spec 的意圖宣告產生，方向正確性由單元測試保證。
3. **sensor 是 reconciliation 模型**（desired − observed），狀態存 event log，cursor 只存指紋。禁止 append-only watermark。見 §5。
4. **LSF 走 Pipes 層**，launcher 維持近預設。禁止「自寫 LSFRunLauncher + asset 內 Pipes」並用（nested bsub）。見 §6。
5. **run_key 用 `hashlib`，不用 `hash()`**（跨 process 穩定）。
6. **每個模組先寫測試再寫實作（TDD）**。純函數（mapping builder、grouping planner、version 計算）必須有獨立的 pytest，不依賴 Dagster runtime、不碰 LSF、不碰檔案系統。
7. **DAGSTER_HOME 錨在本機磁碟**，不放 NFS。
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
                            │ 觸發 run
┌───────────────────────────▼─────────────────────────────────┐
│  M4  執行層：Launcher（近預設)+ Executor（in_process)        │
│  職責：在 submit 主機起 run worker;run 內調度 step            │
│  誰維護：framework owner（一次性設定,所有 flow 共用)          │
└───────────────────────────┬─────────────────────────────────┘
                            │ asset 內呼叫
┌───────────────────────────▼─────────────────────────────────┐
│  M5  LSF Pipes 層（LSFPipesClient)                            │
│  職責：bsub + 共享 FS 收 Pipes 訊息 + 輪詢 bjobs              │
│  誰維護：framework owner（固定元件,spec 餵參數)              │
└─────────────────────────────────────────────────────────────┘
```

**核心原則：宣告與執行分離。** M1 是「宣告什麼」（flow owner、YAML）；M2–M5 是「如何執行」（framework、程式）。

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
    pipes/
      lsf_client.py               # M5:LSFPipesClient
      bsub.py                     # M5:bsub 組裝 / bjobs 輪詢 / bkill
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

# work units → assets
assets:
  - name: start
    kind: entry                    # 無 partition、無上游的起點
    partitioned_by: []

  - name: netlist_files
    kind: lsf_compute              # 走 LSF Pipes
    script: flows.netlist.script:run_netlist   # ① 指向 flow owner script
    version: content_hash          # ② 基礎版名稱,或 flows.netlist.data_version:my_fn
    partitioned_by: [trio_group]   # 降維:只用粗維度
    work_items: cell               # cell 不當 partition,走 config-carried batch
    depends_on:
      - asset: start
        mapping: all

# batching 策略
batching:
  default: { strategy: fixed, size: 100 }
  overrides: {}                    # 例:netlist_files: { size: 50 }

# LSF 資源(餵給 M5 的固定元件)
lsf:
  default: { queue: normal, cores: 4, poll_interval_s: 30 }
  overrides: {}
```

### 3.2 Spec Schema（Pydantic,framework/spec/schema.py）

**[實作契約]** 用 Pydantic v2 定義 schema，載入時嚴格驗證。關鍵欄位：

```python
from pydantic import BaseModel, Field
from typing import Literal
from enum import Enum

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
    kind: Literal["entry", "lsf_compute", "inline_compute"]
    script: str | None = None              # "module.path:callable"
    version: str = "timestamp"             # 基礎版名 或 "module:callable"
    partitioned_by: list[str] = Field(default_factory=list)
    work_items: str | None = None          # 降維用的細維度名
    depends_on: list[DependencySpec] = Field(default_factory=list)

class BatchingRule(BaseModel):
    strategy: Literal["fixed"] = "fixed"   # FUTURE: weighted, timeout
    size: int = 100

class LSFResource(BaseModel):
    queue: str = "normal"
    cores: int = 4
    poll_interval_s: int = 30

class FlowSpec(BaseModel):
    version: int
    flow_name: str
    dimensions: dict[str, DimensionSpec]
    assets: list[AssetSpec]
    batching: dict[str, ...]               # {default: BatchingRule, overrides: {...}}
    lsf: dict[str, ...]
```

**[實作契約] 驗證規則（schema 載入時就擋掉錯誤,不要等 runtime）：**
- `mapping` 宣告裡引用的維度名，必須存在於上游或下游的 `partitioned_by`。
- `script` 指向的 callable 必須 importable（載入時嘗試 import，失敗即報錯）。
- `work_items` 的維度名必須在 `dimensions` 裡定義為 `dynamic`。
- `depends_on` 的 asset 必須存在於同 spec 的 assets。
- 跑 `test_spec_schema.py`：餵合法/非法 spec，驗證該過的過、該擋的擋。

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

    # lsf_compute:config 帶 work items,內部透過 Pipes 投 LSF
    class _Config(dg.Config):
        items: list[str]

    @dg.asset(
        name=asset_spec.name,
        partitions_def=partitions_def,
        deps=deps,
        code_version=...,                            # 連結到 version 策略
    )
    def _asset(context: dg.AssetExecutionContext, config: _Config,
               lsf: "LSFPipesClient") -> dg.MaterializeResult:
        coarse = context.partition_key
        result = lsf.run(
            context=context,
            command=script_fn.bsub_command(coarse),  # script 提供命令組裝
            extras={"items": config.items, "coarse_key": coarse},
            queue=lsf_cfg.queue, cores=lsf_cfg.cores,
        )
        # per-item 可觀測性:從 Pipes 回報,或在此補
        for item in config.items:
            context.log_event(dg.AssetMaterialization(
                asset_key=f"{asset_spec.name}_item",
                partition=f"{coarse}|{item}",
                metadata={"data_version": version_fn(coarse, item)},
            ))
        return result.get_materialize_result()
    return _asset
```

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
        resources={"lsf": LSFPipesClient.from_config(...)},
        executor=dg.in_process_executor,
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

    @dg.sensor(name=f"{asset_spec.name}_sensor", job=job,
               minimum_interval_seconds=60)
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

## 6. M4/M5 — 執行層 + LSF Pipes

### 6.1 M4 — dagster.yaml（固定設定，所有 flow 共用）

```yaml
# framework/config/dagster.yaml
run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 50            # 對齊 LSF queue limit / (batch × cores)
    dequeue_use_threads: true
    dequeue_num_workers: 4
    tag_concurrency_limits:
      - key: "eda/tool"
        value: "primetime"
        limit: 20

run_monitoring:
  enabled: true
  free_slots_after_run_end_seconds: 300

# run_launcher 省略 → 用 DefaultRunLauncher(近預設)
# executor 在 Definitions 設 in_process_executor
```

**[實作契約]** DAGSTER_HOME 設環境變數指向本機磁碟（如 `/local/dagster_home`），不放 NFS。在 README 寫明。

### 6.2 M5 — LSFPipesClient（air-gapped，file-based）

```python
import dagster as dg, subprocess, tempfile, os, shlex, time

class LSFPipesClient(dg.PipesClient, dg.ConfigurableClass):
    shared_dir: str        # NFS 共享路徑,context/message 走這
    default_queue: str = "normal"
    default_poll_s: int = 30

    def run(self, *, context, command, extras=None,
            queue=None, cores=4, poll_interval_s=None):
        queue = queue or self.default_queue
        poll = poll_interval_s or self.default_poll_s
        ctx_path = f"{self.shared_dir}/ctx-{context.run_id}.json"
        msg_path = f"{self.shared_dir}/msg-{context.run_id}.ndjson"
        with dg.open_pipes_session(
            context=context,
            context_injector=dg.PipesFileContextInjector(path=ctx_path),
            message_reader=dg.PipesFileMessageReader(path=msg_path),
            extras=extras,
        ) as session:
            env = session.get_bootstrap_env_vars()
            script_path = self._write_script(env, command)
            job_id = self._bsub(script_path, queue, cores)
            try:
                while True:
                    state = self._bjobs_state(job_id)
                    if state in ("DONE", "EXIT"):
                        break
                    time.sleep(poll)
                if state == "EXIT":
                    raise dg.Failure(f"LSF job {job_id} EXIT")
            except BaseException:
                self._bkill(job_id)        # 中斷時殺 LSF job,不 orphan
                raise
            return session.get_results()
```

外部 worker 端（flow owner 的 script 用 dagster_pipes，vendor 進去）：

```python
# 在 bsub 出去的環境執行
from dagster_pipes import open_dagster_pipes
with open_dagster_pipes() as pipes:
    items = pipes.extras["items"]
    coarse = pipes.extras["coarse_key"]
    for item in items:
        run_eda_tool(coarse, item)
        pipes.report_asset_materialization(
            asset_key=f"{...}_item",
            metadata={"item": item},
            partition=f"{coarse}|{item}",
        )
```

**[實作契約]**
- 用 `PipesFileContextInjector` + `PipesFileMessageReader`（**禁止** S3/GCS/Azure）。
- 中斷時 `bkill`，不留 orphan job。
- `_bsub` / `_bjobs_state` / `_bkill` 抽到 `bsub.py`，獨立可測（用 fake subprocess）。
- **禁止**同時實作自寫 `LSFRunLauncher`。M4 用 DefaultRunLauncher。

---

## 7. 中斷與恢復（必須驗證的行為）

reconciliation + per-item 記錄的組合，保證任何中斷後自動收斂。**[實作契約] 這些行為要寫成整合測試或手動驗證腳本：**

| 中斷點 | 預期行為 | 驗證方法 |
|---|---|---|
| sensor tick 中途中斷 | 下次 tick 重算 desired−observed，未做的重新發 | 殺掉 daemon，重啟，確認剩餘 batch 被撿起 |
| LSF job EXIT | observed 無此批 → 下次重發 | mock 一個 EXIT，確認下次 tick 重發 |
| batch 部分成功（100 做了 60） | 60 個 item 的 materialization 留存，下次只補 40 | 中途 kill，確認下次 desired−observed = 40 |
| daemon 重啟 | 狀態在本機磁碟可靠，續跑 | 重啟 daemon，確認不遺失、不重做 |

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
  test_bsub.py               bsub 組裝 / bjobs 解析（fake subprocess)

整合測試（用 Dagster 但不碰真 LSF）
  test_generator.py          build_definitions(reference spec) 成功產出 assets/sensors
  test_sensor_integration.py 用 DagsterInstance.ephemeral() + fake registry,
                             驗證 sensor 發出正確數量的 RunRequest
  test_asset_with_fake_pipes 用 PipesSubprocessClient（本機,非 LSF)跑 reference

端到端（手動或 CI,本機模擬)
  用 MultiThread/Default launcher + 本機 subprocess 模擬 LSF,
  跑完整 reference flow:start → sensor → batch → 假運算 → materialization
  驗證中斷恢復(§7)
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

**M4/M5 — 執行 + LSF Pipes（2 天）**
- dagster.yaml（§6.1）+ LSFPipesClient（§6.2）+ bsub.py + versioning 基礎版。
- `test_bsub.py`、`test_versioning.py`、`test_asset_with_fake_pipes` 綠。
- 判準：reference flow 用本機模擬 LSF（PipesSubprocessClient 或 echo/sleep）端到端跑通。

**M6 — 中斷恢復 + 收尾（1 天）**
- §7 的中斷恢復驗證。`_template/` 範本。README（含 flow owner 如何新增 flow）。
- 判準：§8.3 的所有 checkbox 通過。

**總計約 7.5 天。** 真 LSF 接入（換掉本機模擬）是 M6 之後的獨立階段，因為它需要在真叢集上測。

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
2. **StaticPartitionMapping 只能用於兩端都 static** 的維度。dynamic 維度（如 cell）用會報 `can only be defined between two StaticPartitionsDefinitions`。所以 cell 維度走 all_of（AllPartitionMapping），不走 static。
3. **MultiPartitionsDefinition 只能有一個 dynamic 維度**。reference 的 cell 是降維成 config，不在 partition，所以不觸發此限制；但若未來把 cell 放回 partition 要注意。
4. **run_key 用 hash() 會因 PYTHONHASHSEED 跨 process 飄移**，破壞去重。一律 hashlib。
5. **sensor tick 慢**：通常不是 plan_batches（純運算快），而是 (a) event log 查詢沒批次化，或 (b) daemon 對 RunRequest 的 partition 驗證（dynamic partition 查詢）。reference 用 static trio_group 可避開 (b)。
6. **DAGSTER_HOME 在 NFS** 會導致 SQLite locking 問題（alembic exists、tick 慢）。本機磁碟。
7. **nested bsub**：若 launcher 自己 bsub 又在 asset 內 Pipes bsub，一個任務佔兩個 LSF slot。reference 用 DefaultRunLauncher（不 bsub）+ asset 內 Pipes bsub（唯一 bsub），正確。

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
> 弱 agent 走 D2_IMPLEMENTATION_PLAN.md 的 Phase C4 時,逐面向跑這份清單。

### C1 State management

| 必須等價的具體點 | 驗收方法 |
|---|---|
| Run / asset / partition / event log 的儲存形狀 | `dagster instance info`、`dagster run list` 兩端比對 |
| Materialization records 每個 partition 有一筆 | `dagster asset partition status --select <asset>` |
| `DataVersion` 計算結果(content_hash 模式) | 同一 input → 同一 16-byte hash;改 input → hash 變 |
| 跨 process / 重啟後狀態仍然一致 | 重啟 daemon,partition status / data version 不變 |

### C2 Stop & rerun

| 必須等價的具體點 | 驗收方法 |
|---|---|
| 單一 partition rerun (`--partition 'INV|tt_25'`) 只重跑該 leaf | `dagster asset materialize --select characterize --partition …`;只看到該 leaf event |
| Run cancellation 不留 orphan(本機 subprocess / 未來 LSF bkill) | 跑到一半 `dagster run terminate`,確認 worker / subprocess 都死 |
| Batch 部分成功 → 下次 reconciliation 只補未完成的 | (僅 `trigger: reconciliation` 模式適用)mock 一個中途失敗,確認下次 tick 只發剩餘 items |
| `run_monitoring.enabled` 偵測 stuck run | 殺掉 worker、`run_monitoring` 在配置秒數內標 run 為失敗 |

### C3 Job scheduling

| 必須等價的具體點 | 驗收方法 |
|---|---|
| `AutomationCondition.eager()` 觸發等價 | 上游 materialize → 下游被 framework-generated 與 converted 各自 eager rebuild 行為一致 |
| `@sensor minimum_interval_seconds` 兩端一致 | 確認 spec 的 trigger 子欄位 → generator 注入 minimum_interval 數值 |
| Backfill 行為等價 | `dagster asset backfill` 對相同 selection 兩端產生同樣的 partition 集合 |
| Daemon liveness | `dagster-daemon liveness-check` 退 0 |

### C4 Dependency definition

| 必須等價的具體點 | 驗收方法 |
|---|---|
| Style A (`deps=[AssetDep(...)]`) 為 framework 唯一輸出風格 | framework 不可用 `ins=`(會強制 IO load);grep 檢查 |
| `MultiPartitionsDefinition` 方向正確 | `mapping_builder` 單元測試 + 兩端對同一 partition key 跑出同一上游集合 |
| Cross-dimension `MultiToSingleDimensionPartitionMapping(partition_dimension_name=…)` | 對 `characterize(pvt=tt_25, cell=INV)` 兩端比上游 dependency partition keys 集合,應相同 |
| 不出現 `PartitionMapping` subclass | grep `class .*PartitionMapping` 全 repo 0 命中(framework 內部 built-in 組合除外) |
| 維度 / role 命名遵循 MEMORY.md「graph-theory terminology」偏好 | code review:不混用 corner/root 域名 |

### C5 Logs & env status

| 必須等價的具體點 | 驗收方法 |
|---|---|
| 結構化 event log 事件類型集合相同 | `dagster run debug export <run_id>` 兩端比對 event types frequency |
| Compute log(stdout/stderr 收集)能取得 | `dagster run log <run_id>` 兩端都有 |
| Pipes message 通道(asset 內呼叫的子程序)順利回傳 materialization | `pipes.report_asset_materialization` 在兩端產出對應 record |
| `dagster instance migrate` 跑得過(schema 一致) | 跑一次,退 0 |
| `DAGSTER_HOME` 本機磁碟(SQLite 正常) | 確認非 NFS;`alembic exists` 不應出現 |

### 驗收彙整模板(D2 Phase C5 寫進 `flows/liberate-char/EQUIVALENCE.md`)

```
| 面向 | 結構差異(列舉,接受) | 行為差異(必須 0) | 結論 |
|---|---|---|---|
| C1 State            | …                       | …                  | PASS / FAIL |
| C2 Stop & rerun     | …                       | …                  | PASS / FAIL |
| C3 Job scheduling   | …                       | …                  | PASS / FAIL |
| C4 Dependency def   | …                       | …                  | PASS / FAIL |
| C5 Logs & env       | …                       | …                  | PASS / FAIL |
```

5 個面向全 PASS = D2 完成判準之一(對應 D2 計畫 §1.1 的等價性條目)。

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
