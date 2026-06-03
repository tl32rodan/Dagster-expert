# MASTER PLAN — 雙 deliverable + scope 對齊

> **此文件**:統籌本次討論的兩個交付物與執行順序。**現在還在 plan 階段,
> 等你 LGTM 才動手實作。**
>
> **與既有文件的關係**(三份文件各司其職):
> - **本檔(`MASTER_PLAN.md`)** = 整體 scope、雙 deliverable、執行序列、判準
> - `REDESIGN_PLAN.md` = personality v2 設計 + 五層 framework 結構藍圖(設計依據)
> - `examples/liberate-char/MIGRATION_PLAN.md` = Deliverable 2 的**草稿**(會在 Phase D 根據 Deliverable 1 的實作經驗修訂)

---

## 0. 你的要求(我覆述,你檢查)

> 「你的 scope 比 liberate-char 還大。你至少有兩步要做:
>  1. 實作五層框架並自行設定第一層 spec 產生一整個 liberate-char
>  2. 建立 migration plan for internal agent」

我的理解:

- **Deliverable 1** = **我親自實作**完整的五層 framework `liberate_char`(在這個
  repo 裡)+ 自己寫 `flows/liberate-char/spec.yaml` + 證明 framework 從這個 spec
  **自動產出**一整套等價於既有 `examples/liberate-char/converted/` 的 Dagster
  程式碼(包含 assets / partitions / mappings / sensor / Pipes resource)。
- **Deliverable 2** = **一份 migration plan 文件**,交給你內部的 agent
  (Minimax M2.5 / Kimi K2.5),讓那個 agent 在內部把 `char_dagster`(repo 看不到)
  遷上 framework。Plan 必須機械化、checkbox 驅動,弱 agent 可逐項執行。

兩個 deliverable 的差異:**Deliverable 1 是我自己跑通**(repo 內、可 demo),
**Deliverable 2 是寫給別人跑的**(內部、我看不到結果)。Deliverable 1 的實作
經驗會把 Deliverable 2 的 plan 變硬(從理論變成「有人走過」)。

**如果我理解錯了任何一條,請在 §0 直接標註。** 後續所有 Phase 都假設這個 scope。

---

## 1. Scope — 兩個 deliverable

### 1.1 Deliverable 1:`liberate_char` framework + reference 生成 demo

**輸入**:
- 三份上傳文件(`A` 通用提案 / `B` LSF 報告 / `C` 實作白皮書)
- repo 既有 `examples/liberate-char/converted/`(作為**等價性目標**,framework
  生成的東西要功能等價於這份 hand-rolled code)
- 你的設計原則(spec 是 menu、不預設應用形狀;見 `MIGRATION_PLAN.md §3`)

**輸出**(repo 內的程式碼 + 測試 + demo):
```
liberate_char/                         ← 新 top-level dir(或放在 personalities 外?見 §6)
  framework/                           ← 五層 framework,by-layer
    spec/、assets/、sensors/、pipes/、versioning/、config/
    tests/
  flows/
    liberate-char/                     ← 我自己寫的 reference spec
      spec.yaml
      script.py
      data_version.py(可選)
      tests/
  definitions.py                       ← workspace.yaml 入口
  workspace.yaml、pyproject.toml
```

**等價性目標**(Deliverable 1 的「跑通」判準):
1. `build_definitions("flows/")` 在 `dagster dev` 啟動,UI 顯示 liberate-char
   的全部 asset(`template_tcl`、`section_tcl`、`model_card`、`netlist`、
   `cell_list`、`main_tcl`、`characterize`)
2. partition 形狀和 `converted/pipelines/spec/partitions.py` 一致
   (`pvt`、`cell`、`pvt × cell`)
3. partition mapping 行為和 `converted/pipelines/deps.py::characterize_deps()`
   一致(`MultiToSingleDimensionPartitionMapping` 兩個方向各一)
4. `python -m _smoke` 跑通,9 個 characterize leaf 全綠
5. `dagster asset materialize --select characterize --partition 'INV|tt_25'`
   可單 leaf 重跑
6. `diff_proof` 通過(類比 `converted/core/diff_proof.py`):**framework 生成
   的程式碼產出的物件 vs `converted/` 產出的物件,差異只在 path / timestamp**

### 1.2 Deliverable 2:migration plan for internal agent

**形式**:一份精煉的 markdown 文件,從 `MIGRATION_PLAN.md` 重新蒸餾,符合
「弱 agent 機械式執行」的標準。

**輸出**(repo 內,但目標讀者是內部 agent):
```
personalities/flow-cartographer/migration-kit/
  INTERNAL_AGENT_MIGRATION_PLAN.md     ← 主檔
  STEP_BY_STEP_CHECKLIST.md            ← 純 checkbox 版
  EQUIVALENCE_TEST_TEMPLATE.py         ← 內部可改的等價性測試
  SPEC_TEMPLATE.yaml                   ← 內部 char_dagster 的 spec 起點
  SCRIPT_TEMPLATE.py                   ← script.py 範本
  TROUBLESHOOTING.md                   ← 常見坑(對映三份文件附錄)
  README.md                            ← 怎麼帶這份 kit 進內部
```

**目標讀者狀態假設**(內部 agent 是怎樣的環境):
- 完全 air-gapped(無 PyPI / 無 internet)
- shell = tcsh
- Minimax M2.5 / Kimi K2.5 級別,**判斷力低**
- 看得到內部 `char_dagster` 程式碼,但**不能把它 copy 給我**(IP 隔離)
- 有 Dagster 1.13.3 已安裝

**Deliverable 2 的「合格」判準**:
1. 一個從沒看過 framework 的弱 agent,照著 STEP_BY_STEP_CHECKLIST,**不需要問
   我問題**就能把內部 char_dagster 遷上
2. 每個 step 都有「跑什麼指令」+「期望輸出」+「失敗了怎麼辦」三段
3. 整份 kit 不引用任何外部資源(air-gapped friendly)
4. 包含 EQUIVALENCE_TEST_TEMPLATE,讓內部 agent **在內部驗證等價性**(因為
   我看不到內部結果)

### 1.3 兩個 deliverable 的關係

```
Phase A 設計凍結 ─────────────┐
                              │
                              ▼
                  Phase B 我親自實作 framework
                       (Deliverable 1 主體)
                              │
                              ▼
                  Phase C 我自己寫 spec、生成 liberate-char、跑通等價性
                       (Deliverable 1 收尾)
                              │
                              ▼
                  Phase D 從實作經驗蒸餾 migration kit
                       (Deliverable 2)
                              │
                              ▼
                  Phase E personality v2 落地 + master test
                       (兩個 deliverable 都需要新 personality 護航)
```

**為什麼這個順序**:Deliverable 2 必須在 Deliverable 1 之後寫——因為我自己沒
走過 framework 的建構,寫出來的 migration plan 是空想。等我親自把 framework
做出來、自己 spec 一個 flow 跑通,**踩過的坑**會直接變成 TROUBLESHOOTING.md
的內容。

---

## 2. 文件地圖(本次討論已產生 + 將產生的所有計劃文件)

| 檔案 | 角色 | 狀態 |
|---|---|---|
| `personalities/flow-cartographer/MASTER_PLAN.md`(本檔) | 整體 scope、雙 deliverable、執行序列 | **新寫,待你 LGTM** |
| `personalities/flow-cartographer/REDESIGN_PLAN.md` | personality v2 設計 + 五層藍圖(設計依據) | 已寫;Phase A 期間可能微調 |
| `personalities/flow-cartographer/examples/liberate-char/MIGRATION_PLAN.md` | Deliverable 2 的**草稿**(對映 char_dagster → framework 的階段) | 已寫;Phase D 會大幅蒸餾 / 改寫 |
| `personalities/flow-cartographer/migration-kit/*` | Deliverable 2 的最終形式 | Phase D 寫 |
| `personalities/flow-cartographer/FRAMEWORK_CHARTER.md` | personality v2 取代 CONVERSION.md 用 | Phase E 寫 |
| `liberate_char/...` | Deliverable 1 程式碼 | Phase B/C 寫 |

---

## 3. 執行序列 — Phase A..E

> 每個 Phase 結束都要能 **demo + 跑測試**。不可跳階。

### Phase A — 設計凍結(0.5 天,純文件)

**動作**:
1. 你 review 本檔(MASTER_PLAN)、`REDESIGN_PLAN.md`、`MIGRATION_PLAN.md`
2. 對齊三件事:
   - §1.1 Deliverable 1 的 6 條等價性判準是否完整
   - §1.2 Deliverable 2 的合格判準是否完整(特別是「弱 agent 不需問我問題」)
   - §6 部分待決問題(下表)
3. 你寫 LGTM(或標修改),Phase A 凍結

**Phase A 判準**:三份計劃文件 header 加 `STATUS: approved`,可以開工。

### Phase B — Framework 實作(我親自做)(預估 5–6 天)

> **這個 Phase 對映 `MIGRATION_PLAN.md §6` 的 P0–P4**,但實作者改成 Claude。
> 把 char_dagster reference 換成 liberate-char(因為內部 char_dagster 我看不到)。

| 子階段 | 對映 | 主要產出 | 判準 |
|---|---|---|---|
| B0 | P0 骨架 | `liberate_char/` 目錄、`pip install -e .`、`dagster dev` 啟動 | 空 Definitions 載入無錯 |
| B1 | P1 spec schema | `framework/spec/{schema,menu,loader}.py` + 測試 | `test_spec_schema.py` 綠 |
| B2 | P2 generator | `framework/assets/{mapping_builder,partition_builder,builder,version_resolver}.py` + 測試 | `test_mapping_builder.py` 含三方向案例綠;對假 spec 可 build_definitions |
| B3 | P3 sensor + automation | `framework/sensors/{factory,planner,reconcile,automation}.py` + 測試 | 4 個 trigger menu 全有 builder + 測試 |
| B4 | P4 dispatch | `framework/pipes/{subprocess_client,lsf_client,bsub}.py` + 測試 | `dispatch: local` 跑通;`lsf` stub 跑通 |

**Phase B 整體判準**:
- 所有 framework 單元測試綠
- `framework/` 內 0 個 hand-rolled `@asset`、0 個 hand-written `MultiPartitionMapping`
- `framework/` 不 import `flows/` 任何模組(`grep -r "from flows" framework/` = 0)
- 五層契約 C1..C5(`REDESIGN_PLAN.md §5`)全部過

**Phase B 風險**:
| 風險 | 對策 |
|---|---|
| mapping_builder 方向錯 | 強制 `test_mapping_builder.py` 含 weekly_abc→daily_123 反向案例,且 assert dict key 集合 |
| schema 設計沒涵蓋 liberate-char 形狀 | B1 結束前先試寫 `flows/liberate-char/spec.yaml`(Phase C 的前置 dry-run);schema 不夠就回 B1 |
| LSFPipesClient 全新建,容易設計過度 | B4 只做 stub(bsub = local echo),真 LSF 接是 Deliverable 1 之外 |

### Phase C — 用 framework 生成 liberate-char + 等價性驗證(預估 1.5–2 天)

> **這個 Phase 對映 `MIGRATION_PLAN.md §6` 的 P5**,reference 從 char 換成 liberate-char。

| 子階段 | 主要產出 | 判準 |
|---|---|---|
| C1 | `flows/liberate-char/spec.yaml`(我自己寫) | 通過 Pydantic 載入 |
| C2 | `flows/liberate-char/script.py`(從 `converted/pipelines/generators.py` + `core/*` 抽純函數) | `test_script.py` 綠 |
| C3 | `build_definitions("flows/")` 對此 spec 產出 Dagster | UI 顯示 7 個 asset + 正確 partition 形狀 |
| C4 | `flows/liberate-char/tests/test_equivalence.py` | §1.1 的 6 條等價性判準全綠 |
| C5 | `flows/liberate-char/EQUIVALENCE.md` | 對照表:framework asset ↔ `converted/` asset |

**Phase C 判準**(= Deliverable 1 完成判準):**§1.1 的 6 條全綠**。

**Phase C 風險**:
| 風險 | 對策 |
|---|---|
| spec 寫到一半發現 framework 缺欄位 | 回 B1 加 menu 項,擴單元測試(走 menu-extend SOP)|
| 等價性差太多(不是只有 path 差) | C4 把每個差異 dump 出來,逐項決定:接受 / 改 framework / 改 spec |
| `MultiToSingleDimensionPartitionMapping` 的 dimension 方向 | mapping_builder 必須通過 weekly_abc→daily_123 反向測試才可進 C |

### Phase D — 從實作經驗蒸餾 Deliverable 2(預估 1–1.5 天)

**動作**:
1. 把 Phase B/C 我親自踩過的所有坑寫進 `migration-kit/TROUBLESHOOTING.md`
2. 把 `MIGRATION_PLAN.md §6` 的 P0–P5 重新蒸餾為「**內部 agent** 把 char_dagster
   接上**已存在的 framework**」的 step-by-step(不再是「**從零建 framework**」)
   - 因為內部 agent 不需要建 framework(framework 已從 Phase B 來),它只需要:
     - 安裝 framework(pip install 我做出來的 wheel,或 git clone)
     - 寫一份 `flows/char/spec.yaml`(對照內部 char_dagster)
     - 抽 `flows/char/script.py`
     - 跑 EQUIVALENCE_TEST_TEMPLATE.py
3. 產出 `migration-kit/` 的全部檔案(§1.2 列表)
4. 在 `migration-kit/README.md` 第一段寫「**怎麼把這份 kit 帶進 air-gapped 內部
   環境**」(打包、傳輸、解壓的具體指令)

**Phase D 判準**(= Deliverable 2 完成判準):**§1.2 的 4 條全綠**。最關鍵的是
第 1 條(弱 agent 不問問題就能執行)——驗證方法:Phase D 結束前,我自己**扮演弱
agent**,只看 migration-kit,不看 framework 原始碼,把 `flows/liberate-char/`
作為「假內部 flow」走一遍。能走完則合格。

### Phase E — personality v2 落地 + master 整合(預估 1 天)

**動作**(對映 `REDESIGN_PLAN.md §7` 的 Ph1+Ph2):
1. 落地 personality v2 的全部新檔(`REDESIGN_PLAN.md §6.1`)
2. 改寫舊檔(`REDESIGN_PLAN.md §6.2`),把 ROLE / TICK_GUIDE / STATUS 切到 v2
3. 廢除歸檔(`REDESIGN_PLAN.md §6.3`)
4. 把 verify-loop 升級成 C1..C5 + Cross + Smoke 的機械化檢查
5. **跑一次 master test**:對 `flows/liberate-char/` 跑完整 plan→build→verify→reflect
   一輪,確認 v2 personality 能正確驅動 framework

**Phase E 判準**:
- `STATUS.md::allmight_status: v2`
- `archive/v1/` 含舊文件
- `layer-contracts/M1..M5.md` 落地
- master test 一輪走完無 blocked

---

## 4. 預估總工時

| Phase | 預估 | 工時類型 |
|---|---|---|
| A 設計凍結 | 0.5 天 | 你 review + 我修訂 |
| B framework 實作 | 5–6 天 | 我親自寫 code + 測試 |
| C 生成 demo + 等價驗證 | 1.5–2 天 | 我親自寫 spec + 跑測試 |
| D 蒸餾 migration kit | 1–1.5 天 | 我親自寫文件 + 自我驗證 |
| E personality v2 落地 | 1 天 | 我寫 / 改 personality 檔案 |
| **總計** | **9–11 天** | |

> 註:預估假設「跳過 Phase A 不在工時內」(因為主要是你 review)。
> 也假設**真 LSF 接入是 Deliverable 1/2 之外的後續階段**(`MIGRATION_PLAN.md §6 P6`)。

---

## 5. 每個 Phase 的 demo + checkpoint

| Phase | 你可看到的 demo | checkpoint(我會在這停下等你 ack 才繼續) |
|---|---|---|
| A | 三份計劃文件 LGTM | ✅ 必停 |
| B0 | `dagster dev` 啟動空 Definitions、目錄結構就緒 | 可選停(看你要不要 review 結構) |
| B2 結束 | mapping_builder 測試全綠的輸出 | 可選停 |
| B4 結束 | Phase B 五層契約全綠的 grep 結果 | ✅ 建議停 |
| C 結束 | `python -m _smoke` 全綠、`diff_proof` PASS | ✅ 必停(等價性是核心交付) |
| D 結束 | migration-kit/ 完整 + 我扮弱 agent 自我驗證的紀錄 | ✅ 必停 |
| E 結束 | master test 一輪走完 | 收尾 |

---

## 6. 待決問題(Phase A 要回答)

| 編號 | 問題 | 我的傾向 | 影響 |
|---|---|---|---|
| Q1 | `liberate_char/` 放哪?repo 新 top-level dir / `personalities/flow-cartographer/liberate_char/` / 完全新 repo? | **repo 新 top-level dir**(因為它是 framework,服務多 personality)| 影響 import 路徑、workspace.yaml |
| Q2 | Deliverable 2 的 migration-kit 是否要 vendored framework(把 framework 程式碼一起打包進內部)? | **要**(air-gapped 環境;wheel 不可用) | 影響 kit 大小與更新流程 |
| Q3 | reference flow 用 `liberate-char` 還是另起 `demo` 名? | **`liberate-char`**(對齊 examples/liberate-char/converted/ 的等價性目標) | 影響命名一致性 |
| Q4 | M5 的 LSFPipesClient 在 Deliverable 1 內要不要試接真 LSF? | **不**(stub 即可;真 LSF 是後續階段)| 影響 Phase B4 工時 |
| Q5 | `flows/liberate-char/script.py` 怎麼處理 mock liberate / mock bsub? | **直接複用 `converted/core/{lsf_submit,bin/liberate,bin/bsub}.py`**(它們已是 stdlib mock) | 影響 C2 工時 |
| Q6 | Deliverable 2 的 kit 在 repo 內,還是另起一份 send-out 文件包? | **repo 內 `personalities/flow-cartographer/migration-kit/`**,你後續打包成 tarball 帶進內部 | 影響傳輸 |
| Q7 | personality v2 落地(Phase E)是否真的要在 Deliverable 1/2 之後做? | **是**(避免兩邊同時改;framework 穩定後再升 personality)| 影響時序 |
| Q8 | 已寫的 `MIGRATION_PLAN.md` + `REDESIGN_PLAN.md` 是否要先合進 master?還是 Phase A 後一起修訂? | **Phase A 後修訂**(MASTER LGTM 後一起調 header 與引用) | 影響 Phase A 工時 |

**請對 Q1–Q8 回 yes / no / 替代方案。** 沒回的我會用「我的傾向」推進。

---

## 7. 風險與權衡

### 7.1 高風險

| 風險 | 後果 | 對策 |
|---|---|---|
| Phase C 等價性差太多 | 整個 framework 設計被推翻 | C 結束前 hard stop;若 §1.1 6 條有任何一條失敗,先回頭改 framework 而非 spec |
| schema 設計不夠通用,只服務 liberate-char | 加新 flow 又要改 framework | 用 menu enum 強制收斂,任何 freeform 字串視為設計失敗;Phase A 結束前必須 review menu 覆蓋哪些「想像中」的 flow 形狀 |
| Deliverable 2 的弱 agent 假設過樂觀 | 內部 agent 卡住來問 | Phase D 末「我扮弱 agent」自我驗證,凡是我都要查程式碼才知道的步驟,都該寫進 kit |

### 7.2 已被你回應排除的風險

- LSF dispatch 從零做(你說目前是本機 subprocess)→ Phase B4 只做 local;
  LSFPipesClient 是 stub
- trigger 策略要強制 reconciliation → 你說 menu 化,reconciliation / automation / watermark / manual 共存
- partition 強制細粒度或強制降維 → 你說 framework 不預設,spec 選

### 7.3 還沒明確的權衡

- **Deliverable 2 多硬?** 寫成「弱 agent 完全不問問題」的 kit 工時會增加 50%。
  替代方案是「弱 agent 可問 ≤3 個問題」,工時減少。**Phase A 請對此決策**。

---

## 附錄 A — Deliverable 1 的 spec 草案(我會在 Phase C1 寫的東西)

```yaml
# liberate_char/flows/liberate-char/spec.yaml
version: 1
flow_name: liberate-char

dimensions:
  pvt:
    type: static
    values: [tt_25, ff_125, ss_m40]
  cell:
    type: static
    values: [INV, BUF, NAND2]

defaults:
  trigger: automation
  dispatch: local
  version: content_hash

assets:
  - { name: template_tcl,  kind: generator, script: flows.liberate_char.script:gen_template,  partitioned_by: [pvt], folder_as_asset: false }
  - { name: section_tcl,   kind: generator, script: flows.liberate_char.script:gen_sections,  partitioned_by: [pvt], folder_as_asset: true  }
  - { name: model_card,    kind: generator, script: flows.liberate_char.script:gen_modelcard, partitioned_by: [pvt] }
  - { name: netlist,       kind: generator, script: flows.liberate_char.script:gen_netlist,   partitioned_by: [cell] }
  - { name: cell_list,     kind: generator, script: flows.liberate_char.script:gen_cell_list }
  - { name: main_tcl,      kind: generator, script: flows.liberate_char.script:gen_main_tcl }
  - name: characterize
    kind: compute
    script: flows.liberate_char.script:run_characterize
    partitioned_by: [pvt, cell]
    depends_on:
      - { asset: template_tcl, mapping: { pvt:  identity } }
      - { asset: section_tcl,  mapping: { pvt:  identity } }
      - { asset: model_card,   mapping: { pvt:  identity } }
      - { asset: netlist,      mapping: { cell: identity } }
      - { asset: cell_list,    mapping: all }
      - { asset: main_tcl,     mapping: all }
    trigger: automation
    op_tags: { "dagster/concurrency_key": liberate_run }
```

這份 spec(< 50 行)在 Deliverable 1 跑通後,應該等價於既有 `converted/pipelines/`
全部七個檔案(總約 400 行 hand-rolled Dagster)。

---

## 附錄 B — Deliverable 2 的綱要(內部 agent 看的東西)

`migration-kit/INTERNAL_AGENT_MIGRATION_PLAN.md` 結構草案:

```
# Migration plan(內部 agent 版)

## 0. 你是誰、你看到什麼、你要做什麼(20 行)
## 1. Pre-flight(必跑;7 個 box)
   □ pip install ./liberate_char-vendored
   □ pytest framework/tests/ 應全綠
   □ ...
## 2. Step 1:盤點你內部的 char_dagster
   - 列出每個 @asset 的:name / partitioned_by / deps / trigger / dispatch
   - 寫進 INVENTORY.yaml(template 給)
## 3. Step 2:把盤點翻成 flows/char/spec.yaml
   - 用 SPEC_TEMPLATE.yaml 起手
   - 對照 menu 表填欄位
   - 跑 framework/spec/loader 驗證
## 4. Step 3:抽 script.py
   - 每個 char_dagster asset body 的「真正做事」部分 → script.py 純函數
   - 不寫 @asset 不寫 Pipes 不寫 PartitionMapping
## 5. Step 4:跑 EQUIVALENCE_TEST_TEMPLATE
   - 對任意一個 partition,framework 版 vs hand-rolled 版產出比對
   - PASS / FAIL 對應的處理
## 6. Step 5:切流
   - 平行雙跑(framework + hand-rolled 同時跑一段時間)
   - 確認沒問題後切棄 hand-rolled
## 7. 常見問題(TROUBLESHOOTING.md 摘錄)
## 8. 求救(無法解 → 寫進 OPEN_QUESTIONS.yaml,我會在 PR 看到)
```

---

## 附錄 C — 廢除舊文件處置

| 舊文件 | 處置 |
|---|---|
| `personalities/flow-cartographer/CONVERSION.md`(v1 placeholder)| Phase E 移到 `archive/v1/`,加 deprecation header 指向 `FRAMEWORK_CHARTER.md` |
| `flow-model/_plan.yaml`(v1 ledger)| Phase E 移到 `archive/v1/`,新位址 `framework-model/_plan.yaml` |
| `examples/liberate-char/converted/`(v1 hand-rolled Dagster)| **保留**,加一份 `README_V2.md`:「這是 framework v2 的等價性對照目標,不再是推薦的開發方式」 |
| `examples/liberate-char/MIGRATION_PLAN.md`(我之前寫的草稿)| Phase D 內容大部分蒸餾進 migration-kit;原檔保留但加 deprecation header |
| `personalities/flow-cartographer/REDESIGN_PLAN.md`(我之前寫的)| 維持,Phase E 期間是 personality v2 的設計依據 |

---

**等你 review。** 主要看點:
1. §1.1 / §1.2 對 Deliverable 1 / 2 的定義
2. §3 Phase 序列(尤其是 Phase B/C/D 的順序)
3. §6 的 Q1–Q8 待決問題(請回答)
4. §7.3 Deliverable 2 弱 agent 假設要多硬

修改點請直接在本檔 inline 標註,或回信指明節號。LGTM 後 Phase A 凍結,Phase B 可以開始。
