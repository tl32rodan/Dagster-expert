# REDESIGN PLAN — flow-cartographer 圍繞五層 framework 重新打造

> **此文件**:flow-cartographer personality 的重新設計藍圖。把核心從
> 「raw flow → Dagster code」的轉換 loop,改為**圍繞五層 framework
> `liberate_char` 的維護 + 多 flow 接入 + spec-driven code generation**。
>
> **核心命題**:**flow owner 只動第一層(spec)。Spec 寫 `trigger`、
> `dispatch`、`partitioned_by` 等 switch,framework 把 M2/M3/M4/M5 的 Dagster code
> 全自動生成。** flow-cartographer personality 的存在價值,就是讓這套 generation
> 機制可被弱 agent 機械式地建構、驗證、維護、演進。
>
> **與既有文件的關係**:
> - 五層架構定義 = 三份上傳文件 `A/B/C`(已內化於 `MIGRATION_PLAN.md §1–§3`)
> - `MIGRATION_PLAN.md` = 在新 personality 下的**第一個應用案例**(char_dagster → liberate_char)
> - 本文件 = personality 本身怎麼運作的設計文件
>
> **讀者**:你(決策者)+ Minimax M2.5 / Kimi K2.5(實作者)。
> **狀態**:草案;落地前需你 review §3 的 tick 語意改寫與 §6 檔案改寫清單。

---

## 0. 變更摘要 — 舊 vs 新 flow-cartographer

| 維度 | 舊(`v1`,現行) | 新(`v2`,本計劃) |
|---|---|---|
| 心智模型 | 「我把 raw flow 一塊一塊轉成 Dagster code」 | 「我維護五層 framework,並透過 spec 接入多個 flow」 |
| 主要產物 | hand-rolled Dagster project | `liberate_char/` 五層 framework + N 個 `flows/<name>/spec.yaml` |
| 輸入 | `$FLOW_SRC` + `CONVERSION.md` | `liberate_char/` 程式碼 + `flows/*/spec.yaml` + `FRAMEWORK_CHARTER.md`(取代 charter) |
| 終止條件 | `CONVERSION.md` success criteria 全綠 | **無單一終點**;framework 持續演進,每個 flow 有自己的 `spec_status: stable` |
| ticks 語意 | plan/build/verify/reflect 圍繞 conversion increment | plan/build/verify/reflect 圍繞**五層 framework + flow spec** |
| verify 判準 | 6 條 conversion 檢查(invented-api / private-import / smoke / not-converted / uncited / coverage-gap) | **改寫為「五層契約」檢查**(見 §5) |
| handoff | `STATUS.md` + `flow-model/_plan.yaml`(單一 flow ledger) | `STATUS.md` + `framework-model/_plan.yaml`(framework backlog)+ `flows/<name>/_plan.yaml`(每 flow 一份 spec ledger) |

**舊 personality 沒被廢掉,而是被「重新定位」**:它的 plan→build→verify→reflect
loop 仍然存在,但每個 tick 的工作內容圍繞五層轉。

---

## 1. 設計核心 — 五層 framework 是 personality 的主軸

### 1.1 五層定義(這就是 personality 服務的對象)

```
┌──────────────────────────────────────────────────────────────────┐
│  M1  Spec 層(YAML 配置)                                          │
│  ── flow owner 唯一接觸的層;framework 對其他層做 code gen 的開關 │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ 驅動以下全部產生
┌────────────────────────────────▼─────────────────────────────────┐
│  M2  Definitions 產生器(framework/assets/、framework/generator) │
│  ── spec → @asset / partitions / mappings / Definitions          │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ 注入 / 影響
┌────────────────────────────────▼─────────────────────────────────┐
│  M3  Grouping Sensor / AutomationCondition 層                    │
│  ── 由 spec 的 trigger 欄位決定產什麼:                            │
│      automation / reconciliation / watermark / manual            │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ 在固定運行環境下執行
┌────────────────────────────────▼─────────────────────────────────┐
│  M4  執行層(launcher / executor / coordinator / pools)         │
│  ── framework 給統一的 dagster.yaml;spec 餵 op_tags / pool 名     │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ 把運算 dispatch 出去
┌────────────────────────────────▼─────────────────────────────────┐
│  M5  Dispatch 層(local subprocess / LSF Pipes / …)             │
│  ── 由 spec 的 dispatch 欄位決定注什麼 resource                   │
└──────────────────────────────────────────────────────────────────┘
```

### 1.2 personality 對五層的職責

| 層 | personality 對它做什麼 |
|---|---|
| M1 | 維護 schema(`framework/spec/schema.py` + `menu.py`);新 menu 項提案進 reflect tick |
| M2 | 維護 generator;新增 partition / mapping / version 策略 |
| M3 | 維護 sensor / automation 模板;每個 trigger 一個產出函數 |
| M4 | 維護 `framework/config/dagster.yaml`;極少改動 |
| M5 | 維護 dispatch client(local / lsf / …);新增 dispatch 模式 |
| `flows/` | 接受新 flow 接入請求;**只動 spec,不動 framework** |

**personality 不做**:
- 不寫 hand-rolled `@asset`(那是 v1 的工作,已被淘汰)
- 不寫 hand-written `MultiPartitionMapping`(已由 `build_mapping()` 封裝)
- 不替具體 flow 在 framework 寫 `if flow_name == "char": ...`

---

## 2. 核心命題的實作 — 「第一層 trigger 設定完,下四層 code gen」

這是 framework 的中心機制。spec 的每個 switch 都對應 M2–M5 的一塊產出。
本節給出**完整的對映表**——它是 framework 的設計藍圖,也是 verify-loop
的契約來源。

### 2.1 Spec switches → 下層產出的對映表

| Spec 欄位 | 值(menu) | M2 產出 | M3 影響 | M4 影響 | M5 影響 |
|---|---|---|---|---|---|
| `kind` | `entry` | `@asset`(無 partition、無 deps、回 `MaterializeResult()`) | 無 trigger | — | 無 dispatch |
| | `generator` | `@asset` 寫檔案、回 `MaterializeResult(data_version=…)` | trigger 可選 | — | 無 dispatch |
| | `compute` | `@asset` 走 Pipes、回 `result.get_materialize_result()` | trigger 必填 | — | dispatch 必填 |
| `partitioned_by` | `[]` | `partitions_def=None` | — | — | — |
| | `[pvt]` | `partitions_def=pvt_static` | — | — | — |
| | `[pvt, cell]` | `partitions_def=MultiPartitionsDefinition({...})`(1.13.3 max 2 維) | — | — | — |
| `work_items` | `null` | asset 不收 `items` config | trigger ∈ {automation, manual} | — | 一 run 處理一個 partition |
| | `cell`(降維) | asset 收 `Config(items: list[str])`,內部 loop | **強制** trigger = reconciliation 或 watermark | — | 一 run 處理多個 work-item |
| `depends_on[i].mapping` | `"identity"` | 同名同維 → `None`(Dagster 自處理)| — | — | — |
| | `"all"` | `AllPartitionMapping()` | — | — | — |
| | `"last"` | `LastPartitionMapping()` | — | — | — |
| | `{...dim: rule}` | `MultiPartitionMapping` via `build_mapping()`(方向保證) | — | — | — |
| `trigger` | `automation` | asset 加 `automation_condition=AutomationCondition.eager()` | 不產 sensor | — | — |
| | `automation_on_missing` | `automation_condition=AutomationCondition.on_missing()` | 不產 sensor | — | — |
| | `reconciliation` | asset 加 `Config(items=…)`(若 work_items) | 產 sensor + `EventLogSource` + `plan_batches()` | — | — |
| | `watermark` | — | 產 sensor + cursor watermark(legacy 模式) | — | — |
| | `manual` | — | 不產 sensor、不注 automation_condition | — | — |
| `dispatch` | `local` | asset body 注 `PipesSubprocessClient` resource | — | — | `Definitions.resources["pipes"] = PipesSubprocessClient()` |
| | `lsf` | asset body 注 `LSFPipesClient` resource | — | concurrency_key / op_tags 流入 pool | `Definitions.resources["pipes"] = LSFPipesClient.from_config(spec.lsf)` |
| `version` | `timestamp` | `DataVersion(time.time_ns())` | — | — | — |
| | `content_hash` | `DataVersion(sha256(output))` | — | — | — |
| | `input_fingerprint` | `DataVersion(sha256(tool_ver + inputs))` | — | — | — |
| | `"<module:fn>"` | import 自訂 `VersionFn` | — | — | — |
| `op_tags` | `{...}` | `@asset(op_tags={...})` 直傳 | — | `tag_concurrency_limits` / pool 依賴這 | — |
| `batching` | `{strategy: fixed, size: N}` | — | `plan_batches(batch_size=N)` | — | — |
| `folder_as_asset` | `true` | asset 多寫一個資料夾、metadata 加 `folder` | — | — | — |

### 2.2 「設定完 trigger → 下四層自動生」的具體流程

```
flow owner 編輯 flows/char/spec.yaml,改 trigger: automation -> reconciliation
                                ▼
spec/loader.py 載入 + 驗證(menu 檢查 + cross-field 檢查)
   • 例:trigger=reconciliation 但 work_items=null → 載入時報錯
                                ▼
generator.py:build_definitions("flows/")
   ├─ assets/builder.py
   │     for each asset_spec:
   │       partitions_def  = partition_builder.build(asset_spec, dims)
   │       deps            = build_deps(asset_spec, mapping_builder)
   │       automation_cond = automation.build_if(trigger)        # M3 之一
   │       config_class    = build_config(work_items)
   │       pipes_resource  = dispatch.resolve(asset_spec.dispatch) # M5
   │       version_fn      = version_resolver.resolve(version)
   │       @asset(...) ← 全部組裝
   ├─ sensors/factory.py
   │     for each asset_spec with trigger ∈ {reconciliation, watermark}:
   │       sensor = build_sensor(asset_spec, planner, source)     # M3
   ├─ resources/
   │     pipes = dispatch.resolve_all(specs)                      # M5
   │     base config from framework/config/dagster.yaml           # M4
   └─ return Definitions(assets=..., sensors=..., resources=..., executor=in_process)
                                ▼
Dagster 讀 Definitions,UI / CLI / sensor daemon 全部就位
   • flow owner 沒寫一行 @asset / @sensor / PipesClient
   • framework 沒寫一行 if flow_name == "char"
```

### 2.3 為什麼這個機制 = 「弱 agent 也能維護」

弱 agent 改一個 flow 的 trigger,**只需要**:

1. 編輯 `flows/char/spec.yaml`,把 `trigger: automation` 改成
   `trigger: reconciliation`,並加 `work_items: cell` 與 `batching: {size: 100}`。
2. 跑 `pytest framework/tests/test_spec_schema.py`(驗 spec 合法)。
3. 跑 `pytest flows/char/tests/`(驗 reference flow 跑通)。

它**不需要**懂 sensor 怎麼寫、reconciliation 怎麼算 desired−observed、cursor 怎麼存指紋。
那些是 framework 已寫好的、被 trigger menu 觸發的 code generation。

弱 agent 不會犯的錯(因為被 framework 接管):
- mapping 方向反:`build_mapping()` 內封裝,單元測試保證
- nested bsub:framework 不寫 LSFRunLauncher,M5 永遠是 Pipes
- cursor 爆:`reconciliation` 模式強制只存指紋
- `hash()` 不穩定:framework 用 `hashlib`,弱 agent 不接觸 run_key

---

## 3. ticks 語意改寫(v2)

每個 tick 仍是「一個 wake = 一個動作」,但動作的對象從「conversion increment」
改為「**framework 一層的 PR**」或「**flow spec 的 PR**」。

### 3.1 `plan` tick

**輸入**:
- `framework-model/_backlog.yaml`(framework owner 維護的長期 backlog)
- `flows/*/spec_status.yaml`(每個 flow 的待辦)
- 上次 reflect 提案

**動作**:從 backlog 挑下一個 `planned` 增量,具體化為 `_plan.yaml::increments[]`
的下一條,每條需具備:
- `target_layer`: `M1 | M2 | M3 | M4 | M5 | flows.<name>`
- `change_kind`: `new-menu-item | new-builder | bugfix | new-flow | spec-edit | refactor`
- `accept`: 跑哪個測試
- `depends_on`: 其他 increment id

**判準**:`_plan.yaml` 多一條 `planned` increment,寫進
`framework-model/_operations.log`。

### 3.2 `build` tick

**動作**:挑 `_plan.yaml` 中最低 id、`status: planned`、`depends_on` 全 `done` 的
increment,執行**它,且只執行它**。

**規則**:
- `target_layer: M2` 的 increment **只能改** `framework/assets/`、`framework/generator.py`、
  對應測試。動到 `flows/` 視為違規。
- `target_layer: flows.char` 的 increment **只能改** `flows/char/`,動到 `framework/` 視為違規。
- 每個 increment 結束都必須 `pytest <對應測試>` 綠,才能 `built`。

**判準**:`_plan.yaml` 該條變 `built`,有 `framework-model/digest/<date>-<id>.md`
記錄改了什麼。

### 3.3 `verify` tick

**動作**:對剛 `built` 的 increment 跑**五層契約檢查**(§5)。
任一失敗 → `status: blocked` + finding。

### 3.4 `reflect` tick(週期性)

**動作**:meta-pass。看最近 N 個 verify 的 blocked / finding。

**輸出**(寫進 `framework-model/_open_questions.yaml::framework_proposals`):
- 「應該加 menu 項 X」(累積 ≥3 次手寫 X)
- 「應該支援新 dispatch Y」(累積 ≥2 個 flow 提出)
- 「mapping_builder 有未覆蓋情形 Z」
- 「下層生成有效能/正確性問題 W」

reflect 不寫 framework code,只開 PR 提案。

---

## 4. handoff 重新設計 — `STATUS.md` + 兩個 ledger

舊:`flow-model/_plan.yaml`(單一 flow ledger)
新:
- `framework-model/_plan.yaml`(framework 的開發 backlog 與 increment ledger)
- `framework-model/_backlog.yaml`(長期事項池)
- `framework-model/_open_questions.yaml`(framework 級的待辦/疑問,reflect 寫入)
- `framework-model/_operations.log`(append-only audit)
- `flows/<name>/_spec_status.yaml`(per flow:`spec_status: planned | stable | needs-update`,
  與 framework 解耦)

`STATUS.md` 的 frontmatter:
```yaml
allmight_status: v2
last_activity: 2026-06-03T…
active_increment: f.m2.4  # framework-model._plan.yaml id
next_action: "build f.m2.4 — implement build_mapping() identity+all_of case"
```

---

## 5. 五層契約 = 新的 verify-loop 檢查

`v1` 的 6 條 verify 判準改寫為「**五層契約**」。**任一違反 → increment 退回 blocked**。

### C1 — M1 契約(spec 是 menu)

| 檢查 | 機械化方法 |
|---|---|
| 所有 spec 欄位的值都在 menu enum 內 | 載入時 Pydantic 報錯;`pytest test_spec_schema.py` |
| cross-field 一致性(如 `trigger: reconciliation` 必有 `work_items`) | loader 的 cross-validators;`pytest` |
| spec 不出現 Python code(只能是 data) | `grep -P "def |import " flows/*/spec.yaml` 必須 0 命中 |

### C2 — M2 契約(generator 是純翻譯)

| 檢查 | 機械化方法 |
|---|---|
| `framework/` 不 import 任何 `flows/` 具體模組 | `grep -r "from flows\." framework/` 必須 0 |
| `framework/` 不出現 `if flow_name == "..."` 等 hardcode | `grep -r 'flow_name *=='` 必須 0 |
| `MultiPartitionMapping` 方向正確 | `pytest test_mapping_builder.py`(必含 weekly_abc→daily_123 反向案例) |
| 沒有 hand-written `@asset` 在 `flows/` 內 | `grep -r '@asset' flows/` 必須 0 |
| 沒有 hand-written `PartitionMapping` subclass | `grep -r 'class.*PartitionMapping' framework/ flows/` 只允許 build_mapping 內部 |

### C3 — M3 契約(trigger menu 驅動)

| 檢查 | 機械化方法 |
|---|---|
| 每個 menu trigger 都有對應 builder 函數 + 測試 | `framework/sensors/factory.py` + `tests/test_sensor_integration.py` 涵蓋 4 種 |
| sensor cursor 只存指紋(reconciliation 模式) | `grep -r 'json.dumps.*seen' framework/sensors/` 必須 0;`cursor=` 只見 hashlib |
| run_key 用 `hashlib` | `grep -rE 'run_key.*hash\(' framework/` 必須 0 |
| AutomationCondition 不引用 deprecated `AutoMaterializePolicy` | `grep -r 'AutoMaterializePolicy' framework/ flows/` 必須 0 |

### C4 — M4 契約(framework 固定設定)

| 檢查 | 機械化方法 |
|---|---|
| 只有一份 `dagster.yaml` | `find . -name dagster.yaml -not -path '*/store/*'` 應該只有 framework/config/ + flow 各一份(若需覆寫) |
| `DAGSTER_HOME` 不指 NFS | README 警示 + pre-flight 檢查腳本 |
| 不自寫 `RunLauncher` | `grep -r 'class.*RunLauncher' framework/` 必須 0(Default* 不算自寫)|

### C5 — M5 契約(dispatch menu 驅動)

| 檢查 | 機械化方法 |
|---|---|
| 每個 menu dispatch 都有對應 client + 測試 | `framework/pipes/*.py` 涵蓋 menu;`tests/test_asset_with_fake_pipes.py` |
| LSF 不和 launcher bsub 並用(no nested bsub) | grep 不能同時存在 `LSFRunLauncher` 與 `LSFPipesClient.run` |
| `LSFPipesClient` 用 file-based injector / reader | `grep -r 'PipesS3' framework/` 必須 0 |
| 中斷時 `bkill` | `framework/pipes/lsf_client.py` 必有 `except: bkill(...)`(grep 檢查) |

### Cross — Dagster 1.13.3 corpus

(沿用 v1)

| 檢查 | 機械化方法 |
|---|---|
| `from dagster import X` 的 X 存在於 `personalities/dagster-expert/database/dagster-1.13.3/docs/` | 自動 grep |
| 無 `dagster._core/_internal/_private` | 自動 grep |

### Smoke — reference flow

| 檢查 | 機械化方法 |
|---|---|
| `python -m _smoke` 退 0 | reference flow `flows/char/` |
| per-partition 重跑生效 | `dagster asset materialize -m liberate_char.definitions --select <X> --partition <K>` |

---

## 6. 檔案改寫清單(personality 內部)

### 6.1 新增

| 檔案 | 用途 |
|---|---|
| `REDESIGN_PLAN.md`(本檔) | 本計劃 |
| `FRAMEWORK_CHARTER.md` | 取代 `CONVERSION.md`。framework 級長期目標 / 範圍 / 不變量 |
| `framework-model/_plan.yaml` | framework backlog 的 increment ledger |
| `framework-model/_backlog.yaml` | 長期待辦池(plan tick 從這裡挑) |
| `framework-model/_open_questions.yaml` | reflect 寫入的 proposal 區 |
| `framework-model/_operations.log` | append-only audit |
| `skills/spec-edit/SKILL.md` | flow owner 編 spec 的 SOP(新增 tick 類型,或合進 build) |
| `skills/menu-extend/SKILL.md` | 新增 menu 項的 SOP |
| `skills/new-flow-onboarding/SKILL.md` | 新 flow 接入的 SOP(只動 `flows/<new>/`) |
| `conversion-coverage/`→`layer-contracts/` | 改名(見下) |
| `layer-contracts/M1-spec-is-menu.md` | C1 契約細節 |
| `layer-contracts/M2-generator-is-pure.md` | C2 |
| `layer-contracts/M3-trigger-menu.md` | C3 |
| `layer-contracts/M4-fixed-config.md` | C4 |
| `layer-contracts/M5-dispatch-menu.md` | C5 |

### 6.2 改寫

| 檔案 | 改什麼 |
|---|---|
| `ROLE.md` | §0 Wake SOP 保留;§1–§4 改寫圍繞五層;mode/trigger 表移除(沒有 mode,只有 ticks);verify-loop 6 條 → 五層契約 |
| `TICK_GUIDE.md` | 4 個 tick 的新語意(§3) |
| `STATUS.md` | frontmatter `allmight_status: v2`;`active_increment` 形狀改;`next_action` 對應 framework-model |
| `manifest.yaml` | 描述更新;`capabilities` 不變(memory + schedule);`derived_from` 加上 `dagster-ap-auditor` 與 `flow-cartographer-v1` |
| `PRE_FLIGHT_CHECKLIST.md` | 7 個 box 改成「framework 環境」box(DAGSTER_HOME、venv、pip install -e .、framework/tests 跑得起、flows/ 至少一個 spec) |
| `QUICKSTART.{en,zh}.md` | 入門場景改成「我要新增一個 flow」「我要改一個 flow 的 trigger」「我要加一個 menu 項」 |
| `scheduled/{plan,build,verify,reflect}.md` | tick 的新語意 + 新 ledger 路徑 |
| `standards/{clean-code-rules,refusal-patterns,tdd-rules}.md` | 對應五層契約調整 |
| `smoke/*.md` | 對應 reference flow 跑通的 CLI / GraphQL conformance |

### 6.3 廢除 / 歸檔

| 檔案 | 處置 |
|---|---|
| `CONVERSION.md` | 移到 `archive/v1/CONVERSION.md`,加 deprecation header 指向 `FRAMEWORK_CHARTER.md` |
| `flow-model/{_plan,_open_questions,_operations.log}` | 內容歸檔到 `archive/v1/flow-model/`(因為仍有 char_dagster→liberate_char 的歷史價值);新位址 `framework-model/` |
| `conversion-coverage/*` | 內容拆進 `layer-contracts/M1..M5` 並擴充;原檔加 deprecation header 指新檔 |
| `examples/liberate-char/` 整個 example | **不廢除**,但加一份 `STATUS.md`:「這是 v1 raw-flow → hand-rolled 的 reference;v2 看 `MIGRATION_PLAN.md` 把 hand-rolled → spec-driven」 |

### 6.4 保留不動

| 檔案 | 理由 |
|---|---|
| `skills/wake/` | Wake SOP 仍是第一動作;只調整內部步驟順序 |
| `memory/` 整個目錄 | L1/L2/L3 機制不變,只是 corpus 名換成 framework / per-flow |
| `examples/liberate-char/MIGRATION_PLAN.md`(我剛寫的) | 這是 v2 下的第一個應用案例,直接沿用 |

---

## 7. 實作計劃(Phase 順序)

> 這個 Phase 序列與 `MIGRATION_PLAN.md §6` 的 P0..P6 是**正交的**:
> - 這裡的 Phase 是 **personality 自身的 redesign 落地**
> - `MIGRATION_PLAN.md` 的 P 是 **`liberate_char` framework 本身的建構**
> - 兩者並行:先有 personality v2 的指南(本計劃 Ph0..Ph2)再讓它驅動 framework 的建構(MIGRATION P0..P6)

### Ph0 — personality v2 設計凍結(0.5 天)

**輸入**:本文件 + `MIGRATION_PLAN.md` + 你的 review
**輸出**:
- 你對 §3(tick 語意)、§5(五層契約)、§6(檔案改寫清單)的 ack
- 本文件加上 `STATUS: approved`
**判準**:你寫 LGTM(或修改後 LGTM)

### Ph1 — personality v2 檔案落地(1 天)

**輸入**:Ph0 凍結後的本計劃
**輸出**:
- §6.1 全部新增檔案的初版(framework-model/、layer-contracts/、新 skills/)
- §6.2 全部改寫檔案的初版(ROLE.md、TICK_GUIDE.md、STATUS.md、…)
- §6.3 廢除檔案歸檔到 `archive/v1/`
**判準**:
- `STATUS.md` frontmatter 是 `v2`
- 跑 `/wake flow-cartographer plan` 走的是新 Wake SOP
- pre-flight 跑得過(framework 還沒建,但骨架在)

### Ph2 — verify-loop 五層契約上線(1 天)

**輸入**:Ph1 的檔案
**輸出**:
- `skills/verify-loop/SKILL.md` 改寫,新增 C1..C5 + Cross + Smoke 的機械式檢查腳本
- `tests/test_verify_loop.py`:對假 increment 模擬契約檢查
**判準**:對一份故意違反 C2(在 framework/ import flows.char)的增量,verify
正確報 `framework-leak`

### Ph3..PhN — 驅動 framework 建構

從這裡開始,personality v2 開始驅動 `liberate_char` framework 的建構。
sequence 看 `MIGRATION_PLAN.md §6 P0..P6`;每個 P 都是一連串 increment。

每個 increment 都會經過 plan→build→verify→reflect。整個 framework 從 0 到
P5(reference flow 接通)預估 7.5 天(同 `MIGRATION_PLAN.md`)。P6(真 LSF)
是獨立階段。

---

## 8. 新 flow 接入的 SOP(展示 v2 的價值)

> 這是 v2 設計成功的判準:**新 flow 接入完全不動 framework**。

### 步驟

1. 複製 `flows/_template/` 到 `flows/<new_flow_name>/`
2. 編輯 `flows/<new_flow_name>/spec.yaml`:填 menu 欄位
3. 寫 `flows/<new_flow_name>/script.py`:每個 asset 對應的純函數
4. 跑 `pytest flows/<new_flow_name>/tests/`
5. 跑 `dagster dev -m liberate_char.definitions`,確認 UI 看到新 flow 的 asset
6. 提一個 `target_layer: flows.<new_flow_name>` 的 increment 給 personality

### 預期需要的時間

- 簡單 flow(類似 char 的 pvt × cell):**< 4 小時**
- 複雜 flow(多 dispatch、需要新 menu 項):若 menu 已涵蓋則 4–8 小時;
  若需新 menu 項則需先做 `menu-extend` SOP(走 framework increment 路線)

### 對映到 v1 的對照

| v1(現行) | v2(本計劃) |
|---|---|
| 跑 plan tick 拆 increment(N 條,每條 hand-roll 一個 @asset) | 1 條 increment(編輯 spec.yaml + script.py) |
| 7 天以上 | 4 小時 |
| 弱 agent 容易寫錯(mapping 方向、cursor 爆、nested bsub …) | 框架接管;弱 agent 只接觸 spec menu |

---

## 9. 給弱 agent 的執行 checklist

照順序逐項確認:

```
Ph0(設計凍結)
□ 讀完本文件 §0–§8
□ 確認你看過 MIGRATION_PLAN.md 的 §3 設計原則
□ 用戶 LGTM 已寫在本文件結尾

Ph1(檔案落地)
□ framework-model/_plan.yaml 存在,空 increments
□ framework-model/_backlog.yaml 已從 v1 flow-model/_open_questions.yaml 遷移相關事項
□ FRAMEWORK_CHARTER.md 已填(取代 CONVERSION.md)
□ ROLE.md 第一段 ack 圍繞五層 + spec-driven code gen
□ TICK_GUIDE.md 寫 plan / build / verify / reflect 在 v2 的語意
□ STATUS.md frontmatter allmight_status: v2
□ archive/v1/ 含 CONVERSION.md / flow-model/* / conversion-coverage/*
□ layer-contracts/M1..M5 五份檔案存在

Ph2(verify-loop 五層契約)
□ skills/verify-loop/SKILL.md 含 §5 全部 C1..C5 機械化檢查
□ 對假 increment 模擬一輪 verify,故意違反 C2 應報 framework-leak
□ test_verify_loop.py 綠

Ph3 起(framework 建構)
□ 看 MIGRATION_PLAN.md §6 P0..P6
□ 每個 increment 都跑完整 plan→build→verify→reflect

每個 tick 必須(沿用 v1)
□ 讀 STATUS.md 第一行的 next_action
□ 結束時 rewrite STATUS.md + framework-model/_operations.log
□ journal 寫到 memory/journal/framework/<date>-<tick>-<title>.md
□ 不寫到 personalities/dagster-expert/ 內(stay in your lane)

五層契約合規(每個 build 後)
□ C1 spec 是 menu (Pydantic 載入無錯)
□ C2 framework 不 import flows;mapping 方向有測試
□ C3 trigger menu 每個值都有 builder + 測試
□ C4 一份 dagster.yaml;不自寫 RunLauncher
□ C5 dispatch menu 每個值都有 client + 測試;LSFPipes 不和 launcher 並用
```

---

## 附錄 A — 廢除概念的處置

### A.1 「conversion increment」消失

v1 假設 increment 是「把 raw flow 的一塊轉成 Dagster」。v2 沒有 raw flow,
所以這個概念被取代為:
- **framework increment**:改 M1..M5 某一層
- **flow increment**:改 `flows/<name>/spec.yaml` 或 `script.py`

### A.2 「coverage 5 aspects」變「五層契約」

v1 的 `conversion-coverage/01..05` 是「conversion 必須涵蓋的 5 個行為面向」
(state / stop&rerun / scheduling / dependency / logs)。v2 改為「framework
五層契約 C1..C5」。內容移到 `layer-contracts/M1..M5.md`,擴充新檢查。

5 aspects 的本意(行為要 preserved)沒有消失:它變成 reference flow(`flows/char/`)
的 `EQUIVALENCE.md` 測試。

### A.3 `CONVERSION.md` 消失,`FRAMEWORK_CHARTER.md` 取代

`FRAMEWORK_CHARTER.md` 寫的是 framework 級的不變量:

```yaml
# 例:framework charter 的形狀
charter:
  framework_name: liberate_char
  invariants:
    - "framework/ 不 import flows/ 任何具體 flow"
    - "flow owner 不接觸 Dagster API"
    - "spec 是 menu,不是 freeform"
    - "M5 永遠走 Pipes,絕不自寫 RunLauncher"
    - "Dagster 1.13.3 public API only"
  growth_policy:
    new_menu_item:
      threshold: "累積 ≥3 個 flow 需要 → reflect 提案 → plan 排 increment"
    new_layer:
      threshold: "需要正式 RFC"
```

---

## 附錄 B — 新增 menu 項的 SOP(`menu-extend`)

當 reflect 累積 ≥3 個 flow 需要某個 menu 項(例如新的 `trigger: deadline`):

1. `plan` tick 開一個 `target_layer: M1` 的 increment:
   - 在 `framework/spec/menu.py::Trigger` 加新 enum 值
   - 在 `framework/spec/schema.py` 的 cross-validators 補規則
   - 寫 `test_spec_schema.py` 新案例
2. `plan` tick 開一個 `target_layer: M3` 的 increment(若該 menu 項落在 M3):
   - 在 `framework/sensors/factory.py` 加對應 builder
   - 寫 `test_sensor_integration.py` 新案例
3. `plan` tick 開一個 `target_layer: flows.<example>` 的 increment:
   - 在 reference flow demo 新 menu 項的 spec
4. 上述 3 個 increment 全 done 後,新 menu 項才算「上線」
5. 廣播給其他 flow owner(在 `FRAMEWORK_CHARTER.md::release_notes` 加一行)

---

**等你 review。** 修改點請直接在本文件 inline 標,或回信指明節號。
LGTM 後 Ph0 凍結,Ph1 即可開始落地。
