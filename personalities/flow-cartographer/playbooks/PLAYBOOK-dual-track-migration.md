# PLAYBOOK — 雙軌 pipeline 遷移到 Execution Fabric

> **場景**:同一目標的兩套實作並存——(a) 可 sequential run 的 DAG
> pipeline scripts(介面清晰);(b) 內部 Dagster pipeline(機制已融入
> 需求場景,但分層不明確)。要把它解構重組到 5-layer Execution Fabric,
> 且:**scripts 不可變**(未來由下游 owner 控管,只交 schema;平台只做
> schema 驗證)、**Ops 雙軌並跑**(sequential ↔ fabric 交叉驗證)。
>
> **執行者**:DEV personality(Minimax-class agent 或 developer)照本
> 操作;OPS 接手 P6。**教練**:flow-cartographer。**Dagster API 查證**:
> dagster-expert(SKILL 強制查 corpus,0 命中 ⇒ REFUSE)。
>
> **法源**:`/WHITEPAPER.md`——§3.0 五層總表、§3.4/§3.5 fabric 介面、
> §5 遷移方法論、§7 correctness、§10 rejected designs。本 playbook 是
> §5 的**場景特化**,不取代它;衝突時 WHITEPAPER 為準,並回報 ambiguity。
>
> **本 playbook 的閱讀方式**:P0→P6 依序執行;每個 phase 結尾有
> **EXIT GATE(命令+預期輸出)**,GATE 不過就不准進下一個 phase;
> 「REFUSE」條目是硬規則——觸發時停下、輸出指定 remediation 文字,
> 不要用判斷力繞過。

---

## 0. 參數表(開工前填完;全部絕對路徑)

DEV 第一個動作:把本表填進 `$MIG_WORK/migration_state.yaml`(模板見 §0.2)。

| 參數 | 意義 | 填入值 |
|---|---|---|
| `FLOW_NAME` | 新 flow 名(小寫snake) | ☐ |
| `SEQ_ROOT` | sequential pipeline scripts 根目錄 | ☐ |
| `SEQ_RUNNER` | sequential 執行入口(一鍵跑全 DAG 的命令) | ☐ |
| `LEGACY_DAG_ROOT` | 內部 Dagster pipeline 實作根目錄 | ☐ |
| `FABRIC_ROOT` | 本 repo `execution_fabric/` 在工作站上的絕對路徑 | ☐ |
| `MIG_WORK` | 遷移工作目錄(ledger/baseline/matrix 都放這) | ☐ |
| `GOLDEN_INPUTS` | 固定 fixture 輸入集(交叉驗證的黃金輸入) | ☐ |
| `K_GREEN` | cutover 判準:連續全綠交叉驗證次數(建議 ≥5) | ☐ |
| `COVERAGE_PCT` | cutover 判準:驗證覆蓋 partition 比例(建議 ≥95%) | ☐ |

```tcsh
setenv MIG_WORK /abs/path/migration-<FLOW_NAME>
mkdir -p $MIG_WORK/baseline $MIG_WORK/conformance $MIG_WORK/schemas
```
(bash: `export MIG_WORK=…; mkdir -p …`)

### 0.1 Phase 狀態機

```
P0 前置檢查 ─→ P1 理解現況 ─→ P2 契約化+凍結 ─→ P3 機制考古
                                   │(FREEZE 之後 scripts 不可變)
P6 雙軌運行 ←─ P5 五層重建 ←─ P4 佈局決策 ←──┘
```

對應 user 的 6 步:P1=步驟1、P2=步驟2、P3=步驟3、P4=步驟4、P5=步驟5、P6=步驟6。

### 0.2 進度帳本(每個 GATE 過了就更新;斷線重來先讀這個)

```yaml
# $MIG_WORK/migration_state.yaml
flow_name: <FLOW_NAME>
params: { seq_root: ..., legacy_dag_root: ..., fabric_root: ...,
          golden_inputs: ..., k_green: 5, coverage_pct: 95 }
phases:
  P0: { status: TODO }          # TODO | IN_PROGRESS | DONE
  P1: { status: TODO, evidence: }   # evidence = GATE 輸出存檔路徑
  P2: { status: TODO, evidence:, freeze_manifest: }
  P3: { status: TODO, evidence: }
  P4: { status: TODO, evidence: }
  P5: { status: TODO, evidence: }
  P6: { status: TODO, green_streak: 0 }
escalations: []                  # P3/P4 的 ESCALATE 條目;非空則 P5 不准開工
```

**Resume 規則(機械)**:讀 ledger → 找第一個 `status != DONE` 的 phase
→ 從該 phase 的第一個未完成 checklist 項繼續。不要憑記憶跳段。

---

## P0 前置檢查(每次 session 都跑,不只第一次)

```tcsh
echo $DAGSTER_HOME                 # 預期:非空
which dagster                      # 預期:venv 內路徑
dagster --version                  # 預期:1.13.10
echo $PYTHONPATH                   # 預期:含 $FABRIC_ROOT
```
(bash: `echo $DAGSTER_HOME; which dagster; dagster --version; echo $PYTHONPATH`)

- `dagster --version` ≠ 1.13.10 ⇒ **REFUSE**:「請 `setenv DAGSTER_VENV
  /abs/venv`(bash: `export DAGSTER_VENV=…`)並 re-source 後重來。」
- P2 完成後,每次 session 加跑:
  ```tcsh
  sha256sum -c $MIG_WORK/scripts.sha256    # 預期:每行 OK
  ```
  任一行 FAILED ⇒ **REFUSE**:「script 在凍結後被改動:<路徑>。revert
  該檔,或取得 user 明確同意後重走 P2。」
- 寫任何 `from dagster import` 之前:先讀
  `personalities/dagster-expert/skills/dagster-1.13.10-airgap/SKILL.md`
  並照 corpus 查證;0 命中 ⇒ REFUSE + 開 lessons inbox。

---

## P1 理解現況(user 步驟 1)

**目標**:兩套實作各自盤點成一張圖 + 一張表;建立 sequential 黃金基線。
**這個 phase 只記錄、不判讀**(判讀是 P3 的事)。

### P1.1 Sequential pipeline 盤點(graph-theory 詞彙)

對 `$SEQ_ROOT` 每個 script 記一列,存 `$MIG_WORK/inventory_seq.tsv`:

| 欄位 | 內容 |
|---|---|
| `node` | script 對應的資產名(= 未來 asset name) |
| `script_path` | 絕對路徑 |
| `parent_of` 邊 | 此 node 的輸出餵給哪些 node(下游) |
| `is_root` | 無上游 ⇒ true |
| `loop_dims` | script 被哪些 loop 變數掃過(= 未來 partition dimensions) |
| `inputs` / `outputs` | 現在實際讀/寫的檔案 pattern |
| `resources` | 現在用什麼 queue/cores/mem/walltime(不知道就標 UNKNOWN) |

### P1.2 Cardinality math FIRST(硬 checkpoint)

按 user 偏好,**先算葉子數再談設計**。存 `$MIG_WORK/cardinality.md`:

```
node × ∏(dimension 值域大小) = leaf 數;全 flow Σ = 總 leaf 數
例:characterize × (3 pvt × 3 cell) = 9
```

- 總 leaf 數 > 100k ⇒ **停**,回報 user 討論分階(WHITEPAPER §5.1-4)。
- 總 leaf 數就是後面所有 `N/N` 驗證的 N。

### P1.3 黃金基線(baseline)——跑兩次

用 `$GOLDEN_INPUTS` 跑 `$SEQ_RUNNER` **兩次**,每次對每個
(node, partition) 的輸出算 digest,存
`$MIG_WORK/baseline/digests_run1.tsv`、`digests_run2.tsv`:

```
格式:node <TAB> partition_key <TAB> sha256
partition_key 正規化(重要):維度依「維度名的字母序」排序,值以「|」
串接——與 Dagster multi-partition key 格式一致,否則 P6 對帳會全部
對不上。出處:database/dagster-1.13.10/docs/ASSETS_PARTITIONS.md §3。
```

```tcsh
diff $MIG_WORK/baseline/digests_run1.tsv $MIG_WORK/baseline/digests_run2.tsv
# 預期:無輸出(完全一致)
```

- 有差異 ⇒ 該 node 的 script **非決定性**(常見:輸出內嵌 timestamp)。
  記入 `$MIG_WORK/nondeterministic.list`,P2 處理;**不准無視**。
- 兩次都成功後,`cp digests_run1.tsv digests.tsv` 作為正式 baseline。

### P1.4 內部 Dagster impl 盤點(只列清單)

```tcsh
grep -rn "@asset\|@op\|@job\|@sensor\|@schedule\|RetryPolicy\|IOManager\|PartitionsDefinition\|resource" $LEGACY_DAG_ROOT --include='*.py' > $MIG_WORK/inventory_legacy.txt
wc -l $MIG_WORK/inventory_legacy.txt     # 預期:> 0
```

**EXIT GATE P1**:四個檔案存在且非空——`inventory_seq.tsv`、
`cardinality.md`、`baseline/digests.tsv`(行數 = 總 leaf 數,
`wc -l` 驗證)、`inventory_legacy.txt`。更新 ledger。

---

## P2 Script 契約化 + 凍結(user 步驟 2)

**目標**:訂出 script 規範 → 必要修改(唯一允許改 script 的窗口)→
sequential 重驗 → **FREEZE**。凍結後 script 是平台的**外部輸入**,
平台只驗 schema、不看內容——這正是未來下游 owner 控管的形狀。

### P2.1 SCRIPT_CONTRACT v1(存 `$MIG_WORK/SCRIPT_CONTRACT.md`,交 user 簽核)

1. **CLI 固定格式**:`<script> --input <abs>... --output <abs>...`。
   可重複多次;不接受其他資料性參數(維度值編碼在路徑裡,由
   schema 的 `path_template` 宣告)。
2. **Exit code**:0 = 成功;非 0 = 失敗。缺 input ⇒ 必須非 0,不准
   產出部分結果後回 0。
3. **只寫宣告的 outputs**(+ 自己 output 目錄下的 scratch)。
4. **決定性**:同 inputs ⇒ 同 outputs digest。做不到的,在 schema 的
   `digest.recipe` 宣告萃取規則(見 P2.2),使「有效內容」決定性。
5. **禁止**:script 內 `bsub`(WHITEPAPER 硬規則;framework 用
   `lsf_run_client` 包)、網路存取、寫 status DB、import dagster。
6. **冪等可重跑**:重跑同 partition 就地覆寫,不留半成品。
7. stdout/stderr 只當 log,不當資料通道。

### P2.2 Schema sidecar(下游 owner 未來要交的東西;現在 DEV 代填)

每個 script 一份 `$MIG_WORK/schemas/<node>.schema.yaml`:

```yaml
schema_version: 1
script: { path: /abs/tool_x.py, interface: cli_v1 }
asset:
  name: <node>
  partitioned_by: [<dim>, ...]          # [] = unpartitioned
  inputs:                               # 每項對應一個 --input
    - { from_asset: <parent_node>, path_template: "/abs/.../{dim}/in.dat",
        mapping: identity }             # identity | all | last(WHITEPAPER §5.1-3)
  outputs:                              # 每項對應一個 --output
    - { path_template: "/abs/.../{dims}/out.dat" }
  digest:
    recipe: whole_file_sha256           # 預設
    # 或 extract_line: "^digest "       # liberate 式:取 digest 行
    # 或 exclude_regex: "^# generated at " # 排除 timestamp 行再 hash
  resources: { queue: normal, cores: 4, mem_mb: 4096, walltime: "24:00" }
dimensions:
  <dim>: { type: static, values: [...] }
```

- **平台只做 schema 驗證** = (a) 這份 YAML 過 Pydantic(P4 併入
  spec.yaml 後由 `framework/spec/schema.py` 驗);(b) P2.3 conformance
  suite 黑箱驗行為。兩者之外平台不審 script 內容。
- 缺欄位 ⇒ **REFUSE** 並輸出缺欄位清單給 script owner(refusal as
  a feature:附「請補 <欄位> 後重交」)。
- 注意:YAML 中日期樣字串(如 `2026-07-01` 作 dimension 值)在
  1.13.10 保持字串不被轉成 datetime(1.13.9 修正;出處:
  `database/dagster-1.13.10/docs/1_13_10_RELEASE_NOTES.md`)。schema
  驗證仍應斷言 `values` 全為 str。

### P2.3 Conformance suite(RED first,黑箱)

`$MIG_WORK/conformance/test_contract_<node>.py`,每個 script 六條:

| # | 檢查 | 方法 |
|---|---|---|
| C1 | 接受 `--input/--output` 且 fixture 上 exit 0 | subprocess.run |
| C2 | 宣告的 outputs 跑完全部存在 | Path.exists |
| C3 | 沒寫宣告外的檔案 | sandbox 目錄前後快照 diff |
| C4 | 決定性:跑兩次 digest(依 recipe)相等 | 兩次 run + 比較 |
| C5 | 靜態掃描無 `bsub|ssh|curl|wget`(二進位 script 改標 `opaque: true`,靠 C3/C4 兜底) | grep |
| C6 | 缺 input ⇒ exit 非 0 | 拿掉一個 input 跑 |

先寫測試(全 RED)→ 才進 P2.4。

### P2.4 必要修改(唯一窗口)

只為讓 script 過 C1–C6 而改;每改一個,單獨 commit,訊息註明
「pre-freeze normalization: <node>」。`nondeterministic.list` 上的
node 在此處理:能改就改成決定性;不能改(下游 owner 的黑箱)就在
schema 補 `digest.recipe` 萃取規則,並讓 C4 以 recipe 判定。

### P2.5 Sequential 重驗

改過任何 script ⇒ 重跑 P1.3 兩次;新 baseline 取代舊檔,並在 ledger
註記「baseline rebuilt @ <ISO>」。沒改的 node digest 應不變:

```tcsh
diff <(sort $MIG_WORK/baseline/digests.tsv) <(sort $MIG_WORK/baseline/digests_new.tsv)
# 預期:只有被修改的 node 出現差異;其他行零差異
```

### P2.6 FREEZE

```tcsh
sha256sum <所有 script 絕對路徑> > $MIG_WORK/scripts.sha256
sha256sum -c $MIG_WORK/scripts.sha256    # 預期:每行 OK
```

**EXIT GATE P2**:conformance 全 GREEN(`pytest $MIG_WORK/conformance -q`
→ `N passed`)+ `scripts.sha256` 存在 + baseline 重驗完成 + user 已
簽核 SCRIPT_CONTRACT。此後改 script = REFUSE(見 P0)。

---

## P3 內部 Dagster impl 機制考古(user 步驟 3)

**目標**:把 legacy impl 的每個機制分成「設計(要移植)」vs
「將就/legacy(要丟)」。**用查表,不用判斷力。**

對 `inventory_legacy.txt` 每一命中,填一列
`$MIG_WORK/mechanism_matrix.tsv`:

| 欄位 | 填法 |
|---|---|
| `mechanism` | 名字(如 RetryPolicy on characterize) |
| `evidence` | file:line |
| `in_seq_too` | sequential 版也有等價物?Y/N |
| `layer` | 按 WHITEPAPER §3.0 五層總表,它「該」屬於 M1–M5 哪層(或 OPS/無) |
| `violates` | 是否踩到該層「不做什麼」欄?Y/N |
| `rejected` | 命中 §10 哪一條?(10.1–10.11 / 無) |
| `verdict` | 按下面規則機械判定 |

**Verdict 規則(依序套用,第一條命中即停)**:
1. `rejected` 非空 ⇒ **DROP**(記 §10.x)。
2. `layer` 對且現在就寫在對的位置 ⇒ **KEEP**。
3. `layer` 對但寫錯位置(如 compute 塞在 asset body)⇒ **RELOCATE**(記目標層)。
4. 對不上任何層 ⇒ **ESCALATE**(寫進 ledger `escalations`,停給 user)。

**「設計 vs 將就」輔助判準**(填 `in_seq_too` 後看):
- 兩套都有 ⇒ 傾向核心設計(KEEP/RELOCATE 後移植)。
- 只在 dagster 版且 `violates=Y` ⇒ 傾向將就寫法(多半 DROP/RELOCATE)。
- 只在 dagster 版且乾淨對應某層 ⇒ 這就是「已融入我們場景的機制特色」,
  是本次遷移要保住的東西(KEEP,P4 記佈局)。

**已知典型對應(直接抄,不用重推)**:

| Legacy 機制 | verdict | 去處 |
|---|---|---|
| `RetryPolicy` / `run_retries` | DROP(§10.9) | 重試語義=dispatch sensor 下一 tick desired−observed 自然重投 |
| `@schedule` / cron 內嵌 | DROP(§10.10) | 外部觸發去動 upstream 輸入,data_version 變化自然驅動 |
| IOManager 傳遞資料 | RELOCATE | path convention(sidecar `path_template`);scripts 直接讀寫檔案 |
| asset body 內跑重計算 | RELOCATE → M5 | fabric_worker + inner argv |
| Pipes(`PipesSubprocessClient`) | DROP(§10.3) | worker 直寫 status DB |
| 自製 RunLauncher | DROP(§10.2) | DefaultRunLauncher + 非阻塞 bsub |
| 讀 materializations 當 observed | DROP(§10.4) | dispatch sensor 只讀 status DB |
| PartitionsDefinition / 維度定義 | KEEP → M1 | spec.yaml `dimensions` |
| 失敗告警 / metadata 附掛 | KEEP → M3/OPS | harvest sensor `metadata` + RUNBOOK |

**EXIT GATE P3**:matrix 每列都有 verdict;`escalations` 清單完整輸出
給 user。**ESCALATE 未清空前,P5 不准開工**(P4 可以先做非爭議部分)。

---

## P4 機制特色 → 五層佈局決策(user 步驟 4)

**目標**:把 P3 的 KEEP/RELOCATE 落到 M1–M5,產出 spec.yaml 草案。

1. **佈局表** `$MIG_WORK/layout.md`:每個 KEEP/RELOCATE 機制一列——
   `mechanism → 目標層 → 實作載體(spec 欄位 / generator / sensor /
   worker / runbook 條目)`。
2. **spec.yaml 合成(機械)**:把 `schemas/*.schema.yaml` 合併成
   `$FABRIC_ROOT/flows/<FLOW_NAME>/spec.yaml`:
   - 每份 sidecar 的 `asset` 段 → spec `assets[]` 一項;`inputs[].from_asset`
     → `depends_on`;`resources` → `lsf`。
   - **圖檢查(全部機械)**:asset 名唯一;每個 `from_asset` 都能解析;
     DFS 驗無環;列出 `is_root` 節點。任一失敗 ⇒ REFUSE 附衝突清單。
3. **RED test 先行**(WHITEPAPER §5.2):
   `tests/test_<FLOW_NAME>_spec.py::test_graph_shape` —— assert asset
   集合、每個 compute 的 `partitioned_by`、`depends_on` 集合 == P1.1
   inventory。先 RED,改 spec.yaml 到 GREEN。

**EXIT GATE P4**:`pytest tests/test_<FLOW_NAME>_spec.py -q` → `passed`;
layout.md 覆蓋 matrix 全部 KEEP/RELOCATE 列(逐列打勾)。

---

## P5 五層重建(user 步驟 5)——TDD ladder 特化版

**與 WHITEPAPER §5.6 的差異**:scripts 已凍結,所以「script 純函數」級
變成兩件事——(a) **argv builder** 純函數(把 partition 值 + sidecar
path_template 組成 `--input/--output` inner argv;**不含邏輯**);
(b) P2 的 conformance suite(已 GREEN,凍結後不重寫)。

| 順位 | 測試 | GREEN 判準 |
|---|---|---|
| 1 | `test_<flow>_spec.py`(P4 已過) | graph 形狀 == inventory |
| 2 | `test_<flow>_argv.py` | 每 (asset, partition):argv[0]==script path;`--input/--output` 展開正確;`"bsub" not in argv`;`grep -E "^(from|import) dagster" flows/<flow>/script.py` 0 命中 |
| 3 | (承接)P2 conformance 全 GREEN | 不重寫、不放寬 |
| 4 | `test_<flow>_worker.py` | fabric_worker 的 digest recipe 直譯器:3 種 recipe 各一 fixture;失敗路徑 mark_failed 後 re-raise(WHITEPAPER §3.5) |
| 5 | framework 既有 `test_status_db.py` / `test_lsf_run_client.py` 保持 GREEN | 不改框架;要改 ⇒ 先開 lessons inbox |
| 6 | `test_harvest_sensor.py`(over-test)+ 本 flow partition-key 正規化 case | cursor 只在 report+mark 都成功後前進;key 格式 == corpus ASSETS_PARTITIONS.md §3 |
| 7 | `test_dispatch_sensor.py` with 本 flow dimensions | desired−observed;observed 只來自 status DB |
| 8 | `scripts/run_demo_<flow>.py`(mock bsub) | `N/N SUCCESS`、`N/N materializations`(N = P1.2 leaf 數) |
| 9 | **equivalence test**:demo 產出的 status DB dump vs `$MIG_WORK/baseline/digests.tsv` | `0 mismatches` |

**第 9 級是本場景的靈魂**:fabric_worker 的 `data_version` 用**與
baseline 完全相同的 digest recipe** 計算,於是「sequential vs fabric
等價」退化成一句 join/diff。dump 命令(reference SQLite;Postgres 換
psql):

```tcsh
sqlite3 -separator '	' $DAGSTER_HOME/fabric_status.db \
  "SELECT asset_name, partition_key, data_version FROM tasks WHERE state='SUCCESS'" \
  | sort > $MIG_WORK/digests_fabric.tsv
diff <(sort $MIG_WORK/baseline/digests.tsv) $MIG_WORK/digests_fabric.tsv
# 預期:無輸出
```

**P5 REFUSE 規則**:script.py 出現 bsub / dagster import;
`dagster._core.*` 等私有 import;寫任何 `from dagster import` 前未過
corpus 查證;`define_asset_job` 想把混合 partition shape 塞同一
selection(每 asset 一個 job——corpus `1_13_10_RELEASE_NOTES.md`
確認此限制仍在)。

**EXIT GATE P5**:ladder 1–9 全 GREEN;`pytest $FABRIC_ROOT/tests -q`
全 repo 綠;ledger 記 demo 輸出路徑。

---

## P6 Ops 雙軌運行 + 持續驗證(user 步驟 6)

**目標**:Ops 同時理解並運行兩軌,直到 cutover 判準達成。
產出 `$MIG_WORK/RUNBOOK.md`,至少含:

1. **兩軌執行法**:
   - Sequential(oracle):`$SEQ_RUNNER` on `$GOLDEN_INPUTS` → digests。
   - Fabric:daemon 啟停、sensor 狀態檢視、status DB 查詢(附 P0 的
     state checkpoints;每條命令配預期輸出)。
   - Legacy Dagster impl:現行執行方式 + dependency 圖(從 P1.4/P3
     matrix 摘要)——**唯讀理解 + 照舊運行**,直到 cutover;不再加功能。
2. **交叉驗證程序(每週期)**:跑兩軌 → 兩份 digests.tsv → diff →
   `0 mismatches` 則 ledger `green_streak += 1`;否則歸零並走 triage。
3. **Mismatch triage tree(機械)**:
   1. 同 partition 用 oracle 重跑兩次 → 兩次不同 ⇒ script 非決定性
      回歸 ⇒ 附 C4 證據退 script owner。
   2. 比對兩軌實際 input digest → 不同 ⇒ 輸入 staging/stale 問題。
   3. dry-run argv builder vs sequential 呼叫紀錄 → argv 不同 ⇒
      spec/mapping bug,回 P4。
   4. 以上皆同 ⇒ fabric bug ⇒ 開
      `personalities/flow-cartographer/memory/lessons_learned/_inbox/`。
4. **Cutover 判準(全要滿足)**:`green_streak ≥ $K_GREEN`;驗證覆蓋
   ≥ `$COVERAGE_PCT`% partitions;`escalations` 為空;user 簽核。
5. **Cutover 後退場(§5.7 場景修正版)**:
   - 刪:legacy impl 的 scheduling/自查邏輯/檔案式進行中狀態/retry queue。
   - **保留:sequential runner——降級為驗證 oracle**,不再擔任 production
     排程(這是對 §5.7「手寫 scheduling 全刪」的本場景修正:它是
     交叉驗證資產,不是噪音)。
   - Legacy Dagster impl:停 sensors/schedules,唯讀保留一個檢核期後
     歸檔。
6. **Rollback(immutability 的紅利)**:任何時點停 fabric,scripts
   凍結不動 ⇒ sequential 軌立即可跑。此性質由 P0 的
   `sha256sum -c` 持續保證。

**EXIT GATE P6(= 遷移完成)**:cutover 判準達成 + 退場清單執行完 +
ledger 全部 phase DONE。

---

## 附:引用索引

| 主題 | 出處 |
|---|---|
| 五層各層做什麼/不做什麼 | `/WHITEPAPER.md` §3.0 |
| status DB 介面契約 / fabric_worker 職責 | §3.4 / §3.5 |
| 遷移方法論 + TDD ladder 原版 | §5(本 playbook 為特化) |
| idempotency key 三重契約 | §7.1 |
| 被否決的設計(P3 查表用) | §10.1–10.11 |
| multi-partition key 字母序格式 | `database/dagster-1.13.10/docs/ASSETS_PARTITIONS.md` §3 |
| sensor cursor / runless event 語義 | `database/dagster-1.13.10/docs/SENSORS.md` |
| 1.13.10 相關修正(YAML 日期字串、job 限制) | `database/dagster-1.13.10/docs/1_13_10_RELEASE_NOTES.md` |
| 參考實作(照抄結構) | `execution_fabric/flows/liberate_char/` + `execution_fabric/tests/` |
