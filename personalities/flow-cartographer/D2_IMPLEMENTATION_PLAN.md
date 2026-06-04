# D2 Implementation Plan — `liberate-char` application on `spec_dagster` framework

> **Scope**:在 D1 完成的 `spec_dagster`(application-neutral 五層 framework)
> 上,實作 `liberate-char` application 作為**第一個 application** + reference。
> 行為等價於既有 `personalities/flow-cartographer/examples/liberate-char/converted/`
> (hand-rolled 版本)。
>
> **不在 D2 scope**:
> - 建造 `spec_dagster` framework 本身(那是 D1)
> - 內部 `char_dagster` 的遷移(後續再談,可能是 D3)
> - 真實 LSF 接入(`dispatch: lsf` 在 D2 維持 stub;真 LSF 是後續階段)
>
> **唯一依賴**:`spec_dagster` 已完成且測試綠(D1 的 B0–B4 已收斂)、
> `FIVE_LAYER_WHITEPAPER.md` 為唯一規格來源、`examples/liberate-char/converted/` 為等價性目標。

---

## 0. D2 的本質 — 「application 而非 framework」

D2 只動兩種檔案:

| 該動 | 不該動 |
|---|---|
| `flows/liberate-char/spec.yaml` | `spec_dagster/framework/**/*.py` |
| `flows/liberate-char/script.py` | framework 任何測試 |
| `flows/liberate-char/data_version.py`(可選) | `examples/liberate-char/converted/`(等價對照組,不變)|
| `flows/liberate-char/tests/*` | `FIVE_LAYER_WHITEPAPER.md` |
| `flows/liberate-char/EQUIVALENCE.md` | |

若 D2 過程發現 framework 缺欄位,**不可在 D2 內就地補**——退回 D1 加 menu 項
(走 `FIVE_LAYER_WHITEPAPER.md` §0 第 8 條「不要過度設計」+ §A 防錯)後再回 D2。

---

## 1. 完成判準(D2 結束時必須全綠)

1. `flows/liberate-char/spec.yaml` 通過 `spec_dagster.framework.spec.loader.load_all()`
2. `dagster dev -m flows.liberate_char.definitions` 啟動,UI 顯示 **7 個 asset**
   (`template_tcl`、`section_tcl`、`model_card`、`netlist`、`cell_list`、`main_tcl`、
   `characterize`),partition 形狀為 `pvt`、`cell`、`pvt × cell` 三組
3. `dagster asset materialize --select '*'` 退 0,9 個 `characterize` leaf 全綠
4. 單 partition 重跑:
   `dagster asset materialize --select characterize --partition 'INV|tt_25'` 退 0,
   且只觸發該 leaf 的 event
5. 等價性測試 `flows/liberate-char/tests/test_equivalence.py` 全綠(覆蓋
   `FIVE_LAYER_WHITEPAPER.md` 附錄 C 的 C1–C5 五個行為面向)
6. `flows/liberate-char/EQUIVALENCE.md` 填好(行為等價;結構差異被列舉並接受)

---

## 2. Phase 序列(C1–C5)

> Phase 編號沿用 MASTER_PLAN 既定的 C 軌(C = framework 應用到 application 的階段);
> 但 D2 是獨立交付,不再 cross-reference 已廢棄的 MASTER_PLAN。

### C1 — 撰寫 `flows/liberate-char/spec.yaml`(0.5 天)

**輸入**:`examples/liberate-char/converted/pipelines/` 的對照
(`spec/partitions.py`、`deps.py`、`assets.py`、`sensor.py`、`generators.py`、`definitions.py`)。

**動作**:依 `FIVE_LAYER_WHITEPAPER.md` §3 spec schema + 已決定的 menu(spec_dagster
D1 凍結後的版本)寫出對應 spec。範本:

```yaml
version: 1
flow_name: liberate-char

dimensions:
  pvt:  { type: static, values: [tt_25, ff_125, ss_m40] }
  cell: { type: static, values: [INV, BUF, NAND2] }

defaults:
  trigger: automation        # AutomationCondition.eager()(對齊 converted/ 的 characterize)
  dispatch: local            # PipesSubprocessClient(對齊 converted/ 的 pipes_subprocess_client)
  version: content_hash      # 對齊 converted/pipelines/assets.py:_dv

assets:
  - { name: template_tcl, kind: generator, script: flows.liberate_char.script:gen_template,  partitioned_by: [pvt] }
  - { name: section_tcl,  kind: generator, script: flows.liberate_char.script:gen_sections,  partitioned_by: [pvt], folder_as_asset: true }
  - { name: model_card,   kind: generator, script: flows.liberate_char.script:gen_modelcard, partitioned_by: [pvt] }
  - { name: netlist,      kind: generator, script: flows.liberate_char.script:gen_netlist,   partitioned_by: [cell] }
  - { name: cell_list,    kind: generator, script: flows.liberate_char.script:gen_cell_list }
  - { name: main_tcl,     kind: generator, script: flows.liberate_char.script:gen_main_tcl }
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
    op_tags: { "dagster/concurrency_key": liberate_run }
```

**判準**:`python -c "from spec_dagster.framework.spec.loader import load_all; load_all('flows/')"` 退 0。
故意改一處錯誤(例如 `mapping: {pvt: invalid_rule}`),載入時報明確錯。

### C2 — 撰寫 `flows/liberate-char/script.py`(0.5 天)

**輸入**:`examples/liberate-char/converted/pipelines/generators.py` 與
`examples/liberate-char/converted/core/*`(`config.py`、`render.py`、`liberate_inner.py`、
`lsf_submit.py`)。

**動作**:把這些 hand-rolled 程式碼**抽出**成純函數,放進 `flows/liberate-char/script.py`。
規則:
- 函數簽名 = framework 在 spec `script:` 欄位呼叫的契約(由 D1 builder 決定)
- **不 import dagster**,不寫 `@asset`、不寫 `PipesClient`、不寫 `PartitionMapping`
- 對於 `kind: compute`(即 `run_characterize`),函數負責組 LSF 命令參數;
  Pipes session 由 framework 開啟,script 只負責「在 worker 端做什麼」

對應表(完成後寫進 `EQUIVALENCE.md`):

| `converted/` 來源 | `flows/liberate-char/script.py` 對應 |
|---|---|
| `pipelines/generators.py::gen_template` | `gen_template(cfg, pvt) -> str` |
| `pipelines/generators.py::gen_section` × 6 | `gen_sections(cfg, pvt) -> dict[int, str]` |
| `pipelines/generators.py::gen_model_card` | `gen_modelcard(cfg, pvt) -> str` |
| `pipelines/generators.py::gen_netlist` | `gen_netlist(cfg, cell) -> str` |
| `pipelines/generators.py::gen_cell_list` | `gen_cell_list(cfg) -> str` |
| `pipelines/generators.py::gen_main_tcl` | `gen_main_tcl(cfg, root) -> str` |
| `pipelines/assets.py::characterize` body | `run_characterize(cfg, pvt, cell, paths) -> dict`(回 metadata)|
| `core/lsf_submit.py` + `core/bin/{bsub,liberate}` | **保留複用**,從 script.py import |

**判準**:
- `flows/liberate-char/tests/test_script.py` 對每個函數做純函數測試,綠
- `grep -E "^(from|import) dagster" flows/liberate-char/script.py` 0 命中
- `grep -E "@(asset|sensor)" flows/liberate-char/script.py` 0 命中

### C3 — Framework-generated Dagster 跑起來(0.5 天)

**動作**:
1. 寫 `flows/liberate-char/definitions.py`:
   ```python
   from spec_dagster.framework.generator import build_definitions
   defs = build_definitions("flows/liberate-char/")
   ```
2. 寫 `flows/liberate-char/workspace.yaml` 指向上述
3. `setenv DAGSTER_HOME /local/dagster_home/liberate-char-d2`(tcsh;bash: `export DAGSTER_HOME=…`)
4. `dagster dev -w flows/liberate-char/workspace.yaml`

**判準**:UI 顯示 7 個 asset、partition 形狀正確、無 import 錯。

### C4 — 等價性測試(1 天)

**輸入**:`FIVE_LAYER_WHITEPAPER.md` 附錄 C 的 5 面向驗收。

**動作**:寫 `flows/liberate-char/tests/test_equivalence.py`,逐面向斷言:

```
class TestC1_StateManagement:
    def test_event_log_types_match(self):
        """同一份 spec 跑 framework 版 vs 跑 converted/ 版,event log 的
        event_type 集合一致。"""
    def test_data_version_deterministic(self):
        """同 input → 同 16-byte content_hash;改 input → hash 變。"""

class TestC2_StopAndRerun:
    def test_single_partition_rerun_isolated(self):
        """materialize --partition 'INV|tt_25' 只觸發該 leaf 的 event。"""

class TestC3_JobScheduling:
    def test_automation_eager_propagates(self):
        """上游 template_tcl(tt_25) materialize 後,
        characterize(*|tt_25) 在 automation tick 內被排入 run queue。"""

class TestC4_DependencyDefinition:
    def test_characterize_pvt_upstream_set_matches_converted(self):
        """對 characterize(pvt=tt_25, cell=INV) 取上游 template_tcl 的
        partition keys,framework 版本 = {tt_25};和 converted/ 一致。"""
    def test_no_partition_mapping_subclass(self):
        """grep flows/ 與 spec_dagster/ 確認沒有自訂 PartitionMapping subclass。"""

class TestC5_LogsAndEnv:
    def test_compute_log_collected(self):
        """script 內 print 的東西 → dagster run log 取得到。"""
    def test_pipes_materialization_reported(self):
        """run_characterize 內透過 dagster_pipes 報的 materialization
        在 event log 出現。"""
```

**驗證方法**(對應 5 面向):兩端跑同一 spec 對映的場景,比對 event log /
metadata / log content / partition status。差異分類為**結構**(可接受)或
**行為**(必須 0)。

**判準**:`pytest flows/liberate-char/tests/test_equivalence.py -v` 全綠。

### C5 — `EQUIVALENCE.md` 收尾(0.5 天)

**動作**:把 C4 跑出來的結果寫進 `flows/liberate-char/EQUIVALENCE.md`,
按白皮書附錄 C 末的彙整模板填表:

```markdown
# liberate-char on spec_dagster — 等價性報告

對照組:`examples/liberate-char/converted/`(hand-rolled,2026-05-26)
被測組:`flows/liberate-char/`(framework-generated,2026-06-XX)

| 面向 | 結構差異(列舉,接受) | 行為差異(必須 0) | 結論 |
|---|---|---|---|
| C1 State            | framework 自動加 `framework/spec_version` metadata;converted 無 | 無 | PASS |
| C2 Stop & rerun     | (列舉)                   | 無                 | PASS |
| …                  | …                        | …                  | …    |
```

**判準**:5 個面向全 PASS。任一行為差異未處理 → 退回相關 phase 修正。

---

## 3. 預估工時

| Phase | 預估 |
|---|---|
| C1 spec | 0.5 天 |
| C2 script | 0.5 天 |
| C3 generated 跑起來 | 0.5 天 |
| C4 等價性測試 | 1 天 |
| C5 EQUIVALENCE.md | 0.5 天 |
| **總計** | **3 天**(假設 D1 已完成且測試綠)|

---

## 4. 風險

| 風險 | 對策 |
|---|---|
| spec schema 不足以表達 liberate-char 形狀 | C1 第一輪寫到一半就 stop;判斷是「framework 缺 menu 項」(退 D1)或是「liberate-char 形狀沒掌握好」(讀 converted/ 再寫)|
| `MultiToSingleDimensionPartitionMapping` 兩個方向 framework 沒處理對 | C4 的 `test_characterize_pvt_upstream_set_matches_converted` 一定要先跑一次,grep upstream set 對比 |
| C4 結構差異被誤判為行為差異 | 在 `EQUIVALENCE.md` 嚴格區分:「結構」= 自動 vs 手寫的寫法不同(自動加 metadata、log 排列差異)、「行為」= materialize / partition status / data version / rerun isolation |
| `examples/liberate-char/converted/` 的 mock `bsub`/`liberate` 在 framework 版本不能直接 reuse | C2 把 `core/lsf_submit.py` 與 `core/bin/*` 整個 vendor 進 `flows/liberate-char/_lsf_mock/`,framework 走 `dispatch: local` 透過 `PipesSubprocessClient` 呼叫 |

---

## 5. 完成後的下一步(指引,不在 D2 範圍)

D2 完成 ≠ 整個工程結束。完成後可選:
- **D3?** 內部 char_dagster 的 migration kit(從 D2 經驗蒸餾;**等你開**)
- **後續**:真實 LSF 接入 — 由 framework 的 `LSFRunLauncher`(白皮書 §6.2)接管 bsub;application 端 spec 只標 `dispatch: lsf` + `lsf:` 資源(queue/cores/mem_mb/walltime)。**絕不在 application script.py 內再 bsub**(那是 nested bsub;白皮書 §0 第 4 條 + 附錄 A 第 7 條)
- **後續**:把根 `AGENTS.md` / `README.md` / `MEMORY.md` 中 stale 的 v1
  flow-cartographer 描述改成「framework 工作區」說法(本白皮書附錄 D 的
  Ripple 提醒)
