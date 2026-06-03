# Migration Plan — `char_dagster` → `liberate_char`

> **此文件的角色**:你已經內部建好一個可運作的 hand-rolled Dagster(`char_dagster`,
> 尚未在 repo 裡;形狀類似本資料夾 `converted/`)。本文件規劃如何把它**遷移**
> 到一個 spec-driven 通用框架 `liberate_char`。`liberate_char` 是 framework,
> char 是它的第一個 reference flow,但 framework 必須**對其他應用保持中立**。
>
> **讀者**:你(決策者)+ Minimax M2.5 / Kimi K2.5(實作者)。
> **狀態**:草案。下一個 `plan` tick 應根據此文件刷新 `flow-model/_plan.yaml`。
>
> **不重複的引用**:三份上傳文件 = `A`(`universal_dagster_architecture.md`,
> 通用提案 / 全景)、`B`(`dagster_lsf_report_zh_tw.md`,LSF 詳盡報告)、
> `C`(`five_layer_dagster_whitepaper.md`,實作白皮書)。引用 `A§2.3`、
> `B§2.2` 表示「在那份文件的那一節」,本文件不重述,只串聯。

---

## 0. TL;DR(30 秒摘要)

1. **新舊目標差異**:舊 `liberate-char` example 是「raw TCL flow → Dagster」;
   新目標是「hand-rolled Dagster(`char_dagster`)→ spec-driven framework(`liberate_char`)」。
   遷移軸不同,但 repo 既有的 `converted/` 仍是 `char_dagster` 形狀的最佳替身。
2. **架構調和**:repo 既有的 4 層(`spec → rules → registry → translator → factory`,
   見 `personalities/dagster-expert/demo/scale-lib/`)是 `C` 五層裡 **M2(Definitions
   產生器)的內部拆解**,不是衝突。`liberate_char` = 4 層產生器 ⊂ M2;再串
   `A/B/C` 的 M3/M4/M5。
3. **核心設計原則(由你回應確認)**:framework 不預設 partition 形狀、trigger 策略、
   LSF dispatch 模式。**Schema 是 menu**,選擇權在 spec 層(flow owner 在第一層做),
   generator 層根據 spec 分派產生。
4. **遷移分 7 個 phase**(P0..P6),每個 phase 都有判準 + demo + 弱 agent 驗收清單。
   reference flow(char)在 P5 才接上,之前都用 framework 本身的單元測試。
5. **LSF 是全新建置**(你目前是本機 subprocess),所以 P4 不是「移植」而是「按 `C§6` 骨架實作」;
   reference flow 用 `PipesSubprocessClient` 跑通,真 LSF 是 P6 之後的獨立階段。

---

## 1. 為什麼這份文件存在 — 新舊目標的差異

### 1.1 舊目標(repo 既有 example 的形狀)

`personalities/flow-cartographer/examples/liberate-char/` 解的是:

```
flow-src/  (raw Cadence Liberate TCL,hardcoded paths)
   │
   │ flow-cartographer 的 plan→build→verify→reflect loop
   │ 一次轉一個 increment
   ▼
converted/ (hand-rolled Dagster 1.13.3,liberate-specific)
   • pipelines/assets.py    ← 每個 asset 手寫 @asset 函數
   • pipelines/definitions.py ← 明列所有 asset
   • pipelines/deps.py、generators.py、sensor.py
```

`converted/` 是**完全沒有抽象的**:它只服務 liberate-char,新增一個 flow(例如
`real-char`)要從零再 hand-roll 一份相似但又不完全一樣的 code。這就是 `A§1.2`
第 5 項痛點(「依賴關係要好維護、好擴充」)。

### 1.2 新目標 — `liberate_char` framework

你已經在內部有一個 `char_dagster`(repo 裡沒有,但形狀類似 `converted/`)。
目標**不再是把更多 raw flow 轉成 hand-rolled Dagster**,而是:

```
char_dagster  (你內部已建好的 hand-rolled Dagster,liberate-specific 形狀)
   │
   │ 本文件規劃的 migration(7 個 phase)
   ▼
liberate_char/  (spec-driven 通用 framework)
   • framework/        ← 與應用無關,所有 flow 共用
   •   spec/、assets/、sensors/、pipes/、versioning/、config/
   • flows/
   •   char/           ← 第一個 reference flow(原 char_dagster 的 spec 形式)
   •   <future flows>/ ← real-char、其他 EDA、其他應用都掛這
```

關鍵字:
- **framework**(由 framework owner 維護,by-layer)知道**怎麼編譯**spec,
  不知道任何具體 flow。
- **flow**(由 flow owner 維護,by-feature)只提供 `spec.yaml` +
  `script.py`(可選 `data_version.py`),不寫 partition mapping、不寫 sensor、
  不寫 LSFPipesClient。

### 1.3 為什麼不能繼續 hand-roll 下去

`converted/` 的問題,直接對映三份文件提到的痛點:
- 每加一個 flow 都要重寫 `assets.py` / `deps.py` / `sensor.py`(`A§1.2-5`)。
- 每個 asset 的 partition mapping 手寫,**方向反直覺**且只靠工程師記憶
  (`B§1.3` / `C§4.1` / `A§2.3`)。
- 同一份 `LSFPipesClient` 骨架要每個 flow 重複(實際上 `converted/` 還沒
  寫成 client,只是 inline `pipes_subprocess_client.run(...)`,放大規模就會崩)。
- sensor 都是手刻 watermark(見 `converted/pipelines/sensor.py:23-39` 的
  `seen = json.loads(context.cursor or "[]")`),不是 reconciliation,
  踩 `B§1.5` 的 cursor 爆 / 重排破壞 dedup 的坑。

`liberate_char` 把這些痛點的**根因(手寫 + 沒結構)**根治,而不是案例性修補。

---

## 2. 架構地圖 — 5 層 vs 4 層 vs char_dagster 的調和

### 2.1 三份文件講的 5 層(`C§1`)

```
M1  Spec 層(YAML)
M2  Definitions 產生器(spec → assets / partitions / mappings / jobs)
M3  Grouping Sensor 層(reconciliation 或 AutomationCondition,由 spec 選)
M4  執行層(Launcher 近預設 + Executor in_process)
M5  LSF Pipes 層(LSFPipesClient,air-gapped file-based)
```

### 2.2 repo 既有的 4 層參考架構(`scale-lib`)

`personalities/dagster-expert/demo/scale-lib/` 是 repo 已落地的「scale-able
Dagster」reference,4 層:

```
spec        → 宣告(什麼 work、什麼維度、什麼 deps)
rules       → 編譯規則(用什麼 partition、用什麼 mapping)
registry    → 命名解析(把 spec 的名字解成 Dagster 物件)
translator  → 轉成 Dagster API call
factory     → 組 Definitions / 注 sensor / 注 resources
```

外加 **folder-as-asset** 契約(一個資料夾 = 一個 asset 的物理化邊界)。

> ⏳ explore agent 正在摸 scale-lib,回報後本節會補上具體的檔案 + 函數名 +
> signature。**待補:explore 結果**。

### 2.3 調和

```
五層架構(C)                  Repo 既有(scale-lib)              liberate_char 落點
─────────────────────────────────────────────────────────────────────────────
M1 Spec 層                    spec(4 層的第 1 層)              framework/spec/
                                                                  flows/<name>/spec.yaml

M2 Definitions 產生器          rules → registry → translator       framework/assets/
                              → factory(4 層的第 2-4 層)         framework/<…builders>/

M3 Grouping Sensor 層          (4 層未涵蓋,新增)                  framework/sensors/
                                                                  + AutomationCondition 模板

M4 執行層                     (固定 dagster.yaml,所有 flow 共用) framework/config/dagster.yaml

M5 LSF Pipes 層               (4 層未涵蓋,你目前是本機 subprocess)framework/pipes/lsf_client.py
                                                                  (全新建置,reference 先本機)

folder-as-asset 契約          (4 層的橫切原則)                    M2 builder 強制套用
```

**結論**:repo 4 層 ⊂ 五層 M2,M3/M4/M5 是新加的。沒有「兩派架構衝突」這回事。

### 2.4 char_dagster 在這張地圖上的位置

`char_dagster` 目前(根據你回答)的形狀:

| 層 | 你的現況 | 遷移後 |
|---|---|---|
| M1 Spec | 無(都寫在 Python) | 抽到 `flows/char/spec.yaml` |
| M2 Generator | hand-rolled `@asset`(類似 `converted/pipelines/assets.py`) | 由 framework 產生,**flow owner 不再寫 @asset** |
| M3 Sensor | sensor + AutomationCondition 都有(你說的) | 模板化,spec 選一種 |
| M4 執行 | 預設 launcher / executor | 不變(framework 統一給) |
| M5 LSF | 本機 subprocess(你說的) | 全新建置 `LSFPipesClient`;reference 先用 `PipesSubprocessClient` |

遷移**主要是 M1+M2**(抽 spec、寫 generator),M3/M4/M5 是新建。

---

## 3. 設計原則(由你回應提煉、不可違背)

寫死在這裡,實作 agent 違反任一條都算實作失敗。

### 3.1 Schema agnostic — framework 不替應用做決策

> **你的原話**:「現在要架設的通用 dagster 很可能用在其他應用所以 schema 應該也要是
> spec 那一層決定或第二層產生的」

具體規則:

1. **framework 不預設 partition 形狀**。M1 spec 同時支援:
   - 細粒度 partition(每個 work-item 是 partition,如 `converted/` 的 `pvt × cell`)
   - 降維 + config-carried-batch(`C§3.1` 的 `partitioned_by:[粗維], work_items:cell`)
   - 純單 partition / 無 partition
   選哪一種**由 spec 寫**,M2 generator 根據 spec 分派。

2. **framework 不預設 trigger 策略**。M3 模板提供:
   - reconciliation sensor(`C§5`)
   - AutomationCondition.eager / on_missing(repo 既有 1.13.3 API)
   - watermark sensor(legacy,但保留)
   - manual(只有 CLI / UI 觸發)
   spec 寫 `trigger: reconciliation | automation | watermark | manual`,M2 產對應物。

3. **framework 不預設 LSF dispatch 模式**。M5 提供:
   - `PipesSubprocessClient`(本機,reference 用)
   - `LSFPipesClient`(bsub via Pipes,production)
   - `<future>` 別的 cluster manager
   spec 寫 `dispatch: local | lsf | …`,framework 注對應 resource。

### 3.2 Spec 是 menu,不是 freeform

spec schema 的每個欄位都應該是**有限的、可列舉的選項**(以及那些選項需要的參數)。
**不允許**「隨便填一個字串,generator 嘗試解析」這種設計。理由(`A§1.2-1`):
弱 agent 寫 spec 時會用模糊字串,generator 必須在 spec 載入時就擋掉,而不是
runtime 才報錯。

### 3.3 Generator 是 spec → Dagster 的純翻譯器

> generator 不做業務決策,只做機械翻譯。所有判斷(降維、mapping 方向、trigger 選擇)
> 在 spec 層已經寫死。

實作後的判準:`framework/` 裡沒有任何 `if flow_name == "char": ...` 的分支。
如果有,該邏輯應該移到 spec schema 的某個欄位。

### 3.4 Flow owner 不寫 Dagster API

> 包括:`@asset`、`PartitionsDefinition`、`PartitionMapping`、`@sensor`、
> `AutomationCondition`、`PipesClient`。

flow owner 只寫:
- `spec.yaml`(M1)
- `script.py`(描述「這個 work-item 怎麼跑」,純函數或 CLI wrapper)
- 可選 `data_version.py`(自訂版本算法;framework 有 3 個基礎版本可選)

`liberate_char/flows/char/` 必須能用這個介面取代整個 `converted/pipelines/`。

### 3.5 4 個鐵則(整合自三份文件)

1. **partition mapping 方向**:`MultiPartitionMapping` 的 dict key = upstream
   維度名,`DimensionPartitionMapping.dimension_name` = downstream 維度名
   (`A§2.3`、`C§4.1`、附錄 A.1)。這個方向由 `build_mapping()` 封裝,單元測試
   保證,flow owner 永不接觸。
2. **run_key 用 `hashlib`,不用 `hash()`**(跨 process 穩定,`A§1.2-7`、`C§5.3`)。
3. **LSF 走 Pipes 層,不寫 LSFRunLauncher**(避免 nested bsub,`A§2.5`、`B§2.2`、
   `C§6.2`)。
4. **`DAGSTER_HOME` 在本機磁碟,不放 NFS**(SQLite locking,`A§2.7`、`C§6.1`)。

---

## 4. liberate_char 目標 repo 形狀

```
liberate_char/                       # ← 新 repo 或既有 repo 的新 top-level dir
  framework/                         # framework owner 維護,by-layer
    __init__.py
    generator.py                     # 入口:build_definitions(flows_dir) -> Definitions
    spec/
      schema.py                      # Pydantic schema(每個欄位是 menu)
      loader.py                      # 掃 flows/*/spec.yaml,驗證
      menu.py                        # 集中定義所有可選項(partition/trigger/dispatch)
    assets/
      builder.py                     # asset 工廠(@asset 在這裡產出)
      mapping_builder.py             # build_mapping() — 方向正確性的單一保證點
      partition_builder.py           # build_partitions_def()
      version_resolver.py            # resolve_version() 解三種基礎版 + 自訂
    sensors/
      factory.py                     # build_sensor(asset_spec, trigger_spec)
      planner.py                     # plan_batches() 純函數
      reconcile.py                   # ReadinessSource protocol + EventLogSource
      automation.py                  # AutomationCondition 模板
    pipes/
      base.py                        # 共同介面
      subprocess_client.py           # PipesSubprocessClient wrapper(local 用)
      lsf_client.py                  # LSFPipesClient(production,初期 stub)
      bsub.py                        # bsub / bjobs / bkill 包裝,獨立可測
    versioning/
      base.py                        # timestamp / content_hash / input_fingerprint + protocol
    config/
      dagster.yaml                   # 預設 launcher / coordinator / pools
    tests/
      test_spec_schema.py
      test_mapping_builder.py
      test_planner.py
      test_versioning.py
      test_generator.py              # build_definitions 對 reference spec 跑通
      test_bsub.py
      test_sensor_integration.py     # 用 DagsterInstance.ephemeral()
      test_asset_with_fake_pipes.py

  flows/                             # flow owner 維護,by-feature
    char/                            # 第一個 reference flow(= 原 char_dagster)
      spec.yaml
      script.py
      data_version.py                # 可選
      tests/
        test_script.py
    _template/                       # 給未來 flow 複製的範本
      spec.yaml
      script.py

  definitions.py                     # workspace.yaml 指向這裡:defs = build_definitions("flows/")
  workspace.yaml
  pyproject.toml                     # pip install -e .
  README.md
  MIGRATION_NOTES.md                 # 本文件的縮減版,給 flow owner 看
```

**邊界規則**(`C§2` + 你的回應):
- `framework/` **不得 import** `flows/` 任何具體 flow。
- 依賴方向永遠是「flows/ 的 spec 被 framework/ 的 generator 讀取」。
- `framework/` 不認識「char」「real-char」「liberate」等具體應用名稱。

---

## 5. M1 spec schema — menu 而非預設

這是「framework 不替應用做決策」的具體落地。給出可實作的 schema 草案。

### 5.1 完整 schema 草案

```yaml
# flows/char/spec.yaml — 範例(未來會反映你內部 char_dagster 的真實形狀)
version: 1
flow_name: char

# ── 維度宣告 ───────────────────────────────────────────────
dimensions:
  pvt:
    type: static
    values: [tt_25, ff_125, ss_m40]
  cell:
    type: static                       # 也可以 dynamic + source: cell_registry
    values: [INV, BUF, NAND2]

# ── 預設 trigger / dispatch / versioning(可被 asset 覆寫)──────
defaults:
  trigger: automation                  # menu: automation | reconciliation | watermark | manual
  dispatch: local                      # menu: local | lsf
  version: content_hash                # menu: timestamp | content_hash | input_fingerprint | <module:fn>

# ── work units → assets ────────────────────────────────────
assets:
  - name: template_tcl
    kind: generator                    # menu: entry | generator | compute
    script: flows.char.script:gen_template
    partitioned_by: [pvt]
    depends_on: []

  - name: section_tcl
    kind: generator
    script: flows.char.script:gen_sections
    partitioned_by: [pvt]
    # 6 個 section,可選降維:
    # work_items_static: [2, 3, 4, 5, 6, 7]
    # 或保留為 folder-as-asset(一個 asset key = 一個資料夾)
    folder_as_asset: true

  - name: characterize
    kind: compute
    script: flows.char.script:run_characterize
    partitioned_by: [pvt, cell]        # 2D MultiPartitionsDefinition
    depends_on:
      - asset: template_tcl
        mapping: { pvt: identity }     # 1D upstream → 2D downstream
      - asset: section_tcl
        mapping: { pvt: identity }
      - asset: netlist
        mapping: { cell: identity }
    trigger: automation                # 覆寫 defaults
    dispatch: local                    # reference 先 local
    op_tags:
      "dagster/concurrency_key": liberate_run

# ── grouping / batching(只有 work_items 模式需要)───────────
batching:
  default: { strategy: fixed, size: 100 }
  overrides: {}

# ── LSF 資源(dispatch: lsf 時餵給 LSFPipesClient)──────────
lsf:
  default: { queue: normal, cores: 4, poll_interval_s: 30 }
  overrides: {}
```

### 5.2 schema 的 menu 列舉(framework/spec/menu.py 集中定義)

```python
# framework/spec/menu.py
class Kind(str, Enum):
    ENTRY     = "entry"           # 無 partition、無上游、起點
    GENERATOR = "generator"       # 從 config 產生來源檔(folder-as-asset 友善)
    COMPUTE   = "compute"         # 真正的運算 asset(走 Pipes)

class Trigger(str, Enum):
    AUTOMATION     = "automation"      # AutomationCondition.eager()
    AUTOMATION_OM  = "automation_on_missing"
    RECONCILIATION = "reconciliation"  # reconciliation sensor(work_items 模式必用)
    WATERMARK      = "watermark"       # legacy
    MANUAL         = "manual"          # 只 CLI / UI 觸發

class Dispatch(str, Enum):
    LOCAL = "local"   # PipesSubprocessClient
    LSF   = "lsf"     # LSFPipesClient

class VersionStrategy(str, Enum):
    TIMESTAMP         = "timestamp"
    CONTENT_HASH      = "content_hash"
    INPUT_FINGERPRINT = "input_fingerprint"
    CUSTOM            = "custom"   # spec 給 "module:callable"

class MappingRule(str, Enum):
    IDENTITY = "identity"          # 同名同值
    ALL      = "all"               # AllPartitionMapping
    LAST     = "last"              # LastPartitionMapping
    ALL_OF   = "all_of"            # all_of(<dim>) → all per dim
    STATIC   = "static"            # static({"a":"b",…}) 兩端都 static
```

**[實作契約]** schema 載入時擋掉:
- 未在 menu 裡的值
- `trigger: reconciliation` 但 `work_items` 未設(或反之)
- `dispatch: lsf` 但 `lsf:` 區段不存在
- mapping 引用的維度不在 upstream / downstream 的 `partitioned_by`

### 5.3 與你回應的對映

| 你的話 | spec 落點 |
|---|---|
| 「通用架構不該限制 cardinality」 | `partitioned_by` 是 free-list;`work_items` 是 optional;`kind` 不強制 |
| 「liberate char 需反映真實情況」 | reference flow `flows/char/spec.yaml` 用 `pvt × cell` 細 partition(因為 cell 數小)|
| 「現在要架設的通用 dagster 很可能用在其他應用」 | framework 不認識 liberate;`flows/_template/` 給後續應用複製 |
| 「schema 應該是 spec 那一層決定或第二層產生」 | M1 spec 寫意圖;M2 generator 純翻譯;framework 不替應用做選擇 |
| 「sensor + auto cond 都有」 | `Trigger` enum 兩個都列;defaults + asset 級覆寫 |
| 「LSF 還沒接」 | reference 用 `dispatch: local`;`dispatch: lsf` 是 P6 之後 |

---

## 6. 遷移階段(P0..P6)

每個 phase 都應該:**結束時能 demo + 跑測試**。弱 agent 不可跳階。

### P0 — 骨架與決策確認(0.5 天)

**輸入**:本文件 + 你內部 `char_dagster` 的程式碼(請複製一份到 `flows/char/_legacy/`
作為 reference,**只讀**)。
**輸出**:
- 空的 `framework/` + `flows/_template/` 骨架
- `pyproject.toml`、`pip install -e .` 通
- `definitions.py` 回傳空 `Definitions`、`dagster dev` 起得來
- `DAGSTER_HOME` 設好(本機磁碟)
- 一份 `framework/DECISIONS.md` 記錄 §3 設計原則

**判準**:`dagster dev -m liberate_char.definitions` 啟動成功,UI 顯示 0 個 asset。

**風險**:沒有。純骨架。

---

### P1 — M1 Spec schema(0.5–1 天)

**輸入**:§5 的 schema 草案、你內部 char_dagster 的真實形狀。
**輸出**:
- `framework/spec/schema.py`(Pydantic v2)
- `framework/spec/menu.py`(所有 enum)
- `framework/spec/loader.py`(掃 + 驗證)
- `framework/tests/test_spec_schema.py`:餵合法 spec 通過、餵非法 spec 報明確錯
- `flows/char/spec.yaml` 第一個版本(直接從你 char_dagster 推導,不要先簡化)

**判準**:
- `test_spec_schema.py` 全綠
- `python -c "from framework.spec.loader import load_all; print(load_all('flows/'))"` 載入 char spec 不報錯
- 故意把 char spec 改壞(mapping 引用不存在的維度),載入時報明確錯

**風險**:你內部 char_dagster 的某些細節可能塞不進 §5 的 schema。
**對策**:這些細節進 `flow-model/_open_questions.yaml`,**不要為了塞進去而擴大
schema**。等 P5 再看是不是真的需要新欄位。

---

### P2 — M2 Generator 核心(2 天)

**輸入**:P1 的 spec、`personalities/dagster-expert/demo/scale-lib/` 的 4 層
參考(rules → registry → translator → factory 的內部結構)、`C§4` 的所有
[實作契約]。
**輸出**:
- `framework/assets/mapping_builder.py`(§3.5-1 的方向保證)
- `framework/assets/partition_builder.py`
- `framework/assets/builder.py`(`@asset` 工廠)
- `framework/assets/version_resolver.py`
- `framework/generator.py`(`build_definitions(flows_dir)` 入口)
- `framework/tests/test_mapping_builder.py`(必含 §A.1 三個方向案例)
- `framework/tests/test_generator.py`:對 char spec build_definitions 成功

**判準**:
- 所有 mapping_builder 單元測試綠
- `build_definitions("flows/")` 在 `dagster dev` 中可以看到 char 的全部 asset
- partition 形狀正確(`pvt × cell` 是 `MultiPartitionsDefinition`,不是 `Static × Static` 各自分開)
- 隨機抽 2 個 asset,partition mapping 方向用 `m.downstream_mappings_by_upstream_dimension.keys()` 比對 spec 的 upstream 維度名

**風險**(高):mapping 方向錯。**對策**:`test_mapping_builder.py` 必須含 §A.1
的 weekly_abc→daily_123 官方範例的 inverse 案例,並 assert dict key 集合。

---

### P3 — M3 Sensor / AutomationCondition 模板(1.5 天)

**輸入**:你的回應確認 char 用 sensor + AutomationCondition 兩種都有。
**輸出**:
- `framework/sensors/automation.py`:把 `trigger: automation` 翻譯成 `automation_condition=AutomationCondition.eager()` 注入 asset
- `framework/sensors/factory.py`:`trigger: reconciliation` → 產 sensor
- `framework/sensors/planner.py`:`plan_batches()` 純函數(`C§5.1`)
- `framework/sensors/reconcile.py`:`ReadinessSource` protocol + `EventLogSource`
- `framework/tests/test_planner.py`(§C§5.1 必測案例:剛好 batch、尾數、跨 coarse、空、排序穩定)
- `framework/tests/test_sensor_integration.py`:`DagsterInstance.ephemeral()` 跑通

**判準**:
- char spec 改 `trigger: automation`(細 partition 版),materialize 上游後下游
  正確 eager
- char spec 改 `trigger: reconciliation`(work_items 模擬版,用 fake registry),
  sensor 發出預期數量的 RunRequest
- 殺掉 sensor 中途,重啟後重算 desired−observed,**未做的會再被撿起**(`C§7` 中斷恢復)

**風險**:`reconciliation` 模式的 event log 查詢沒批次化 → sensor tick 慢
(`C§5.3` 效能警告)。**對策**:`EventLogSource.observed()` 一次 fetch,
不在迴圈裡 query。

---

### P4 — M4/M5 執行 + Pipes(2 天)

**輸入**:你的回應:目前是本機 subprocess,LSF 還沒接。所以 M5 是**全新建置**,
reference 階段用 `PipesSubprocessClient`。
**輸出**:
- `framework/config/dagster.yaml`(`QueuedRunCoordinator`、`run_monitoring`、
  `tag_concurrency_limits`)
- `framework/pipes/base.py`:共同介面
- `framework/pipes/subprocess_client.py`:wrap `PipesSubprocessClient`(local)
- `framework/pipes/lsf_client.py`:`LSFPipesClient` 骨架(file-based,`C§6.2`),
  **但 stub `_bsub/_bjobs_state` 為本機 echo**(P6 才接真 LSF)
- `framework/pipes/bsub.py`:bsub 組裝 / bjobs 解析(fake subprocess 可測)
- `framework/tests/test_bsub.py`、`test_asset_with_fake_pipes.py`

**判準**:
- char spec 的 `dispatch: local` 跑通(`PipesSubprocessClient`)
- char spec 改 `dispatch: lsf`(stub),也跑通(走 `LSFPipesClient` 但底層是
  本機 echo)
- 中斷時 `bkill`(stub)有被呼叫,不留 orphan

**風險**:lsf_client stub 把 bsub stub 寫死成 echo,**未來真接 LSF 時要小心**。
**對策**:`bsub.py` 的 `submit_bsub/bjobs_state/bkill` 抽成 3 個獨立可 mock 的
函數,P6 用 dependency injection 換真實現。

---

### P5 — char reference flow 完整接入(1.5 天)

**輸入**:你內部 `char_dagster` 的 `script.py`(每個 asset 對應的「實際做事」函數)。
**輸出**:
- `flows/char/script.py`:抽出你內部 char_dagster 中**真正做事**的程式碼,
  包成 `gen_template / gen_sections / run_characterize / …` 純函數或 CLI
- `flows/char/data_version.py`(若你的 char 有非 content_hash 的版本邏輯)
- `flows/char/tests/test_script.py`:純函數的單元測試(不碰 Dagster)
- 一份 `flows/char/EQUIVALENCE.md`:列出 `flows/char/` 和你內部 char_dagster 的
  asset 對照表(每個 char_dagster asset 對應 spec 哪個 asset)

**判準**(這是整個 migration 的核心驗收):
- `python -m _smoke` 或 `dagster asset materialize -m liberate_char.definitions
  --select '*'` 跑通
- 對任意一個 asset(例如 `characterize`),用同一份輸入跑 framework-generated
  版本 vs 原 char_dagster 版本,輸出產物**相同**(或差異只在 path / timestamp,
  類比 `converted/core/diff_proof.py`)
- per-partition 重跑生效(`--partition 'INV|tt_25'` 只跑該 leaf)

**風險**(最高):你內部 char_dagster 有 framework 沒抽象到的細節。
**對策**:每個塞不進的細節進 `flow-model/_open_questions.yaml::charter_proposals`。
**不要**為了塞進去就在 `flows/char/script.py` 寫 monkey-patch。如果某個細節真
代表「framework schema 缺一個欄位」,回到 P1 加 menu 項。

---

### P6 — 真 LSF 接入 + 中斷恢復收尾(獨立階段,>2 天)

**輸入**:LSF 叢集存取、queue 名稱、bsub / bjobs / bkill 真實命令。
**輸出**:
- `framework/pipes/lsf_client.py` 的 `_bsub/_bjobs_state/_bkill` 接真實 LSF
- `dagster.yaml` 調 `max_concurrent_runs` = `min(queue_limit / 1.5, daemon_cap)`
- 中斷恢復四情境驗證(`C§7`):sensor tick 中斷 / LSF EXIT / batch 部分成功 /
  daemon 重啟
- 一份 `flows/char/PRODUCTION_NOTES.md`:LSF queue 配額、預期並發、回滾步驟

**判準**:
- char spec `dispatch: lsf` 在真叢集上跑通 5+ leaf
- 殺掉 daemon,重啟後沒有 orphan LSF job、沒有重做完成的 leaf
- `bjobs` QPS = `Rc / poll_interval`(`A§3.3` 公式)實測在 LSF query rate limit 以下

**風險**:`A§2.6` 的並發旋鈕設錯,要嘛沒吃滿叢集要嘛打爆 queue。
**對策**:用 `A§3.6` 的評估範本算一遍,定 `max_concurrent_runs`,逐次加倍實測。

---

## 7. liberate-char reference flow 的對映

> 這節用本資料夾既有的 `converted/`(=「假設的 char_dagster」)來示範 P5 的對照表
> 應該長什麼樣。你內部真實的 `char_dagster` 比這個複雜,但 shape 一致。

### 7.1 對照表

| `converted/`(hand-rolled) | `flows/char/spec.yaml` 欄位 | framework 產生位置 |
|---|---|---|
| `pipelines/spec/partitions.py::pvt_partitions` | `dimensions.pvt.values` | `framework/assets/partition_builder.py` |
| `pipelines/spec/partitions.py::pvt_x_cell` | `assets[characterize].partitioned_by: [pvt, cell]` | partition_builder 自動 `MultiPartitionsDefinition` |
| `pipelines/assets.py::template_tcl @asset` | `assets[template_tcl]` + `flows/char/script.py::gen_template` | `framework/assets/builder.py` |
| `pipelines/deps.py::characterize_deps()` 手寫 `MultiToSingleDimensionPartitionMapping` | `assets[characterize].depends_on[*].mapping: {pvt: identity}` | `framework/assets/mapping_builder.py` |
| `pipelines/assets.py::characterize` 內 inline `pipes_subprocess_client.run(...)` | `assets[characterize].dispatch: local` + `script: flows.char.script:run_characterize` | `framework/pipes/subprocess_client.py` |
| `pipelines/sensor.py::netlist_drop_sensor`(watermark) | `assets[characterize].trigger: watermark`(或改 `reconciliation`) | `framework/sensors/factory.py` |
| `assets[characterize].automation_condition=AutomationCondition.eager()` | `assets[characterize].trigger: automation` | `framework/sensors/automation.py` |
| `assets[characterize].op_tags={"dagster/concurrency_key": "liberate_run"}` | `assets[characterize].op_tags` | `framework/assets/builder.py` 直傳 |
| `pipelines/generators.py::gen_template` etc. | `flows/char/script.py::gen_template` | flow owner 維護,framework 不碰 |
| `pipelines/definitions.py::defs = Definitions(...)` | 整份 `spec.yaml` | `framework/generator.py::build_definitions` |

### 7.2 P5 結束時的等價驗證

`flows/char/tests/test_equivalence.py`:

```python
def test_asset_set_matches_char_dagster():
    """framework-generated 的 asset 集合 ⊇ char_dagster 的 asset 集合。
    每個 char_dagster asset 在 spec 裡有對應 asset name(或被 folder_as_asset 合併)。"""
    ...

def test_partition_shape_matches():
    """每個 asset 的 partitions_def 物件,key 集合與 char_dagster 一致。"""
    ...

def test_materialize_one_leaf_equivalent():
    """同一份 input,framework 版 vs char_dagster 版的 characterize 輸出相同
    (或差異只在 path / timestamp,類比 converted/core/diff_proof.py)。"""
    ...
```

---

## 8. 與 flow-cartographer 的關係

flow-cartographer 的 `plan → build → verify → reflect` loop **仍然適用**,只是
每個 increment 的判準稍微改:

| flow-cartographer 原本判準(verify-loop §2) | 本 migration 的對應判準 |
|---|---|
| 1 `invented-api`:API 在 1.13.3 corpus | 不變 |
| 2 `private-import`:無 `_core/_internal/_private` | 不變 |
| 3 `smoke-failed`:`python -m _smoke` 退 0 | 不變,但 `_smoke` 走 `build_definitions("flows/")` |
| 4 `not-converted`:必須是 Dagster 不是 copy | **改寫為:必須是 spec-driven 不是 hand-rolled**。若 increment 在 `framework/` 寫了 `if flow_name == "char"`,fail |
| 5 `uncited`:cite `$FLOW_SRC:line` + Dagster API | **改寫為:cite `char_dagster` 對應檔/行 + framework 元件 + spec 欄位** |
| 6 `coverage-gap`:對應 `conversion-coverage/0N-*.md` | 不變 |

**新增一條 verify 判準**:`framework-leak`。`framework/` 不得 import `flows/`
任何具體 flow;`grep -r "from flows.char" framework/` 必須 0 結果。

### 8.1 charter 要怎麼改

`personalities/flow-cartographer/CONVERSION.md` 是 placeholder,要填一份新的:
- `flow_name`: `liberate-char-framework-migration`(或 `char-framework`)
- `$FLOW_SRC`: 你內部 `char_dagster` 的路徑(read-only)
- `Build target`: `liberate_char/`(本文件的 §4 結構)
- `Goal`:「把 hand-rolled char_dagster 遷移到 spec-driven framework
  liberate_char,framework 對應用中立」
- 參考本文件 §6 的 P0..P6 作為 increment 來源

### 8.2 ledger(`flow-model/_plan.yaml`)的形狀

每個 phase 內部拆 ~3–6 個 increment(每個 ≤ 半天可完成),例如:

```yaml
increments:
  - id: p1.1
    title: Pydantic schema + menu enum
    target_phase: P1
    converts: "internal/char_dagster/<top-level>"
    framework_files: ["framework/spec/schema.py", "framework/spec/menu.py"]
    accept: "pytest framework/tests/test_spec_schema.py"
    depends_on: []
    status: planned
  - id: p1.2
    title: loader + 載入 flows/char/spec.yaml
    target_phase: P1
    ...
```

---

## 9. 給實作 agent(Minimax M2.5 / Kimi K2.5)的驗收清單

照順序逐項確認,**任一未過 → 該 phase 不算完成**:

```
P0(骨架)
□ liberate_char/ 結構符合 §4
□ pip install -e . 通
□ dagster dev 啟動成功,顯示 0 個 asset
□ framework/DECISIONS.md 寫進 §3 的設計原則

P1(spec schema)
□ schema.py 用 Pydantic v2
□ menu.py 含 §5.2 全部 enum
□ test_spec_schema.py 含「故意寫錯 mapping 引用維度」案例
□ flows/char/spec.yaml 載入無錯
□ 故意寫 `trigger: foo`,載入時報明確錯

P2(generator)
□ mapping_builder 含 §A.1 三方向案例的單元測試
□ 全 codebase 無 hand-written MultiPartitionMapping
□ build_definitions("flows/") 對 char spec 不報錯
□ Dagster UI 顯示 char 的全部 asset + 正確 partition 形狀
□ framework/ 不 import flows.char 任何符號(grep 0 命中)

P3(sensor / automation)
□ trigger: automation 走 automation_condition 注入
□ trigger: reconciliation 走 sensor + EventLogSource
□ planner 全部單元測試綠
□ sensor tick 在數秒內完成

P4(execution + pipes)
□ dagster.yaml 含 QueuedRunCoordinator + run_monitoring
□ dispatch: local 跑通 PipesSubprocessClient
□ dispatch: lsf(stub)跑通 LSFPipesClient
□ bsub.py 三函數獨立可 mock

P5(char reference flow)
□ flows/char/script.py 抽自你內部 char_dagster,純函數可單測
□ test_equivalence.py 三項全綠
□ per-partition 重跑可動
□ EQUIVALENCE.md 對照表完整

P6(真 LSF,獨立階段)
□ 真叢集跑通 5+ leaf
□ 中斷恢復四情境驗證(§C§7)
□ PRODUCTION_NOTES.md 含 queue 配額 + 回滾步驟

設計原則合規(每個 phase 都要查)
□ schema 是 menu(無 freeform 字串)
□ flow owner 不寫 Dagster API(grep flows/ 無 @asset / @sensor / Pipes 用法)
□ framework 不替應用做決策(grep framework/ 無 if flow_name ==)
□ run_key 一律 hashlib(grep 無 hash() 用於 run_key)
□ DAGSTER_HOME 在本機磁碟
```

---

## 附錄 A — 常見坑(整合自三份文件 + repo 經驗)

### A.1 MultiPartitionMapping 方向(`C§4.1`、`A§2.3`)

**官方範例**:weekly_abc → daily_123:
```python
{"abc": DimensionPartitionMapping(dimension_name="123", partition_mapping=...)}
```

- dict key = `"abc"` = **upstream** 維度名
- `dimension_name="123"` = **downstream** 維度名

`build_mapping()` 內必須對這個方向有 assertion + 單元測試,**不靠工程師記憶**。

### A.2 StaticPartitionMapping 限制

只能用於兩端**都 static** 的維度。dynamic 維度(如 cell registry)用 static 會
報 `can only be defined between two StaticPartitionsDefinitions`。

對策:dynamic 維度走 `mapping: all_of(<dim>)`(`AllPartitionMapping`)。

### A.3 MultiPartitionsDefinition 限制(1.13.3)

- **最多 2 維**(這是 1.13.3 硬限,見 `examples/liberate-char/CONVERSION.md`)。
  spec 寫了 3 維要在 loader 報錯。
- **最多 1 個 dynamic 維度**(`C§9.A.3`)。

### A.4 SQLite / NFS

`DAGSTER_HOME` 在 NFS 會 alembic exists / tick 慢(`A§1.2-6`、`A§2.7`)。
framework `dagster.yaml` 旁邊放 README,**強制本機磁碟**。

### A.5 nested bsub

若 launcher 自寫 LSFRunLauncher + asset 內 Pipes bsub,一個任務佔兩 slot
(`A§2.5`、`B§2.2`、`C§6.2`)。framework 的 M4 用 `DefaultRunLauncher`,
M5 用 Pipes,**永不並用**。

### A.6 hand-written PartitionMapping subclass

社群常見錯誤:subclass `PartitionMapping` 自訂邏輯。這會破壞 reconciliation /
auto-materialize(見 `examples/liberate-char/converted/pipelines/deps.py:9` 的
DON'T 註解)。`build_mapping()` 只組合 built-in。

### A.7 sensor cursor 爆

`B§1.5` / `A§1.2-3`。reconciliation 模式 cursor 只存指紋(SHA256),不存集合本身。
framework 的 `sensors/factory.py` 強制這個模式。

---

## 附錄 B — 架構決策日誌

| 決策 | 取捨 | 採用 | 來源 |
|---|---|---|---|
| 5 層 vs 4 層 | 兩派架構衝突 | **整合**:4 層 ⊂ M2,M3/M4/M5 新加 | §2.3 |
| spec 是否預設 partition 形狀 | 易上手 vs 通用 | **不預設**,schema 是 menu | 用戶回應 |
| trigger 預設 | reconciliation 安全 vs AutomationCondition 簡單 | **menu**,spec 選 | 用戶回應 |
| LSF dispatch | lift-and-shift vs 全新建置 | **全新建置**(你目前是本機 subprocess) | 用戶回應 |
| reference flow | 從零造例 vs 用 char_dagster | **用你內部 char_dagster**(複製到 flows/char/_legacy/ read-only) | §6 P5 |
| folder-as-asset 契約 | 強制 vs 可選 | **可選**(`folder_as_asset: true` spec 欄位) | repo 既有 4 層 |
| 中斷恢復 | reconciliation 自然冪等 vs Dagster RetryPolicy | **reference 用 reconciliation**;長 job 才 RetryPolicy | `C§7` |
| flow-cartographer 適用性 | 重寫 vs 沿用 | **沿用 loop**,改 verify-loop 第 4/5 條判準 | §8 |

---

## 附錄 C — 待補(explore agent 回報後填)

- [ ] `personalities/dagster-expert/demo/scale-lib/` 的 4 層完整檔案 + 函數 +
      signature 拆解,補進 §2.2。
- [ ] 確認 scale-lib 是否已有 mapping_builder 雛形;若有,P2 改成「extend」而非「new」。
- [ ] 確認 scale-lib 的 folder-as-asset 契約具體是怎麼編碼的,把規則寫進
      `framework/assets/builder.py` 而不是再造一套。
