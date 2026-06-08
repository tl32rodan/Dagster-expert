# Mode A Porting 指南 — 把現有 Dagster Project 移植到 spec_dagster Framework

> **目標讀者**:人(專案 owner)+ agent(實作助手)。本指南要讓人讀完
> 能 review agent 的工作,也要讓 agent 讀完能機械化執行 porting。
>
> **適用前提**:你已決定走 **Mode A — sync-execution**(Dagster 同時當
> lineage 與 execution)。如果還沒決定,**先讀 `ARCHITECTURE_CHOICE.md`
> §3 決策樹**。簡言之:同時並發 < ~hundreds + 單 task < 1 hr +
> LSF queue PEND < 30 min → Mode A;否則考慮 Mode B。
>
> **環境前提**:Dagster 1.13.3、tcsh、air-gap、LSF。`spec_dagster/` 已存在
> 且 D1+D2 verified(PR #18)。
>
> **目標**:把你現有的 Dagster project(可能是手寫 `@asset` 散落各檔)
> 收斂成「**一份 spec.yaml + 一份純函數 script.py**」並交給 framework
> 自動產出 Dagster Definitions。

---

## 0. 全局設計框架(必須先讀懂)

spec_dagster framework 把一個 Dagster project 拆成三件事,責任清楚分:

```
┌──────────────────────────────────────────────────────────────┐
│  你寫的:                                                       │
│    flows/<name>/spec.yaml      — 宣告 (dimensions/assets/deps) │
│    flows/<name>/script.py      — 純函數 (無 dagster import)    │
│                                                                │
│  Framework 產出(你不必寫):                                    │
│    @asset / @sensor / Definitions / PartitionMapping           │
│    reconcile / cascade sensor                                  │
│    asset body 內的 PipesSubprocessClient 包裝                  │
│                                                                │
│  Dagster 跑(你只需啟動):                                      │
│    daemon + sensor → run worker → 計算 → MaterializeResult     │
└──────────────────────────────────────────────────────────────┘
```

**核心約束(MUST)**:
1. `script.py` **絕不 `import dagster`**(讓它純函數可單測、可被 Mode A/B 共用)
2. `spec.yaml` 用 **graph-theory** 詞彙(`parent_of` / `ancestor` / `is_root`)
   而非 domain-specific(`corner_of` / `is_corner`)— 詳見 `MEMORY.md` 偏好
3. **Cardinality math first**(MEMORY.md 偏好):porting 第一步是算
   `dim_a × dim_b × ... × dim_n` 的總葉子數。若 > ~hundreds 同時並發,
   停下回去看 `ARCHITECTURE_CHOICE.md` 決定要不要走 Mode B

---

## 1. 需求清單(porting 前你必須有)

| 你必須有 | 為了什麼 |
|---|---|
| 既有 Dagster project 的 asset graph(或 hand-rolled python 檔) | 知道要 port 什麼 |
| 每個 asset 的 partition 形狀(static list / dynamic source / 無 partition) | 寫進 spec.yaml `dimensions:` |
| 每個 asset 的依賴關係 + partition mapping 意圖 | 寫進 spec.yaml `depends_on:` |
| 每個 asset 的計算邏輯(script / 工具呼叫) | 抽到 script.py 純函數 |
| 算 data version 的方法(content hash / 自訂) | 寫進 spec.yaml `version:` |
| LSF 設定:queue、cores、memory、walltime | spec.yaml `lsf:` 區塊(若 dispatch:lsf) |
| 第一個「源頭 asset」(沒有 upstream 的 root)| 驗收時用 `materialize` 觸發 cascade |
| 一份 reference 計算結果(.lib / .ldb / 任何輸出)| 驗收比對 |

---

## 2. 設計抉擇(每個 asset 都要回答)

### 抉擇 1:dimension 是 static 還是 dynamic?

| 條件 | 用 |
|---|---|
| 值在 spec 撰寫時已知,且穩定 | `{ type: static, values: [...] }` |
| 值來自外部(資料庫、檔案系統 scan、env var) | `{ type: dynamic, source: <resource_key> }` |

**踩雷警告**:`MultiPartitionsDefinition` 最多只能有**一個** dynamic 維度
(白皮書附錄 A item 3);若你的兩個維度都是 dynamic,要把其中一個降維成
config 或合併成 composite key。

### 抉擇 2:asset 是 entry / generator / compute?

| 類別 | 用法 | 範例 |
|---|---|---|
| `entry` | 沒有 upstream、不執行任何計算的「源頭」 | data drop 監看點、外部訊號 |
| `generator` | 輕量轉換(< 1 秒);產出檔案;framework 對輸出算 content_hash | 從 config 渲染 template、組 cell list |
| `compute` | 重計算;呼 EDA tool / 模擬器;data version 由 worker 提供 | characterize、simulate、LVS |

### 抉擇 3:Sensor 模型 — Reconcile 還是 Cascade?

| 模型 | spec 寫法 | 行為 | 適用 |
|---|---|---|---|
| **Reconcile** | `trigger: reconciliation` | framework 對每個 compute asset 建一個 sensor;每 30s 比對 `desired - observed`,缺什麼補什麼。需要一條條 materialize upstream | 後台批次補漏;沒人手動 trigger 也會跑完 |
| **Cascade**(預設) | `trigger: automation`(或省略 — 已是 framework default) | framework 建一個 per-flow cascade sensor + 一個 per-asset job;每 10s 比對 desired vs observed,自動 emit 缺失 (asset × partition) 的 RunRequest。**materialize root entry 即觸發整條 cascade** | 「materialize 一個 root → 整條 pipeline 自動跑完」(本指南驗收方式) |

**建議**:porting 時對源頭 asset 標 `kind: entry`(無 script、無 upstream),
其他 asset 用預設 `trigger: automation`,framework 會自動建 cascade sensor
從 root 一路傳到末端。如果你 spec 內全部 asset 都 trigger=reconciliation,
則需要手動 materialize 每個 generator,characterize 才會被 reconcile sensor
看到並 dispatch。

**已驗證的實作雷(L15, L16)**:
- L15:`AutomationCondition.eager()` 在 1.13.3 對「unpartitioned entry →
  partitioned downstream with mapping all」**不會 cascade**(daemon tick 0
  evaluations)— framework 改用自寫 cascade sensor,**porting 時你不必管,
  framework 已處理**。
- L16:`define_asset_job` 不接 selection 跨多 partition shape — framework
  改成 per-asset job,**porting 時你也不必管**。

### 抉擇 4:dispatch:local 還是 dispatch:lsf?

| 條件 | 用 |
|---|---|
| 計算可在 orchestrator host 上跑(輕、快) | `dispatch: local` |
| 計算要 bsub 到 LSF 節點(慢、重) | `dispatch: lsf` + 必填 `lsf:` 區塊(queue/cores/mem_mb/walltime) |

**踩雷警告**:`dispatch: lsf` 在 Mode A 意味著「**LSF run client wrap asset
body**」;不要在 script.py 內自己 bsub(會變 nested bsub,白皮書附錄 A
item 7)。

### 抉擇 5:data version 怎麼算?

| 選項 | 用 |
|---|---|
| `version: content_hash` | 對 generator 輸出 / compute 結果做 SHA256[:16];大多數情況的正解 |
| `version: timestamp` | 每次都不同(用於不該被視為「冪等」的 asset)|
| `version: module.path:fn` | 自訂(例如:讀 .ldb 拿真實 digest) |

**踩雷警告**:對 path-bearing 內容(內含絕對路徑的檔)算 content_hash
會跨環境不一致(L12);把 path 與 content 分離,只算 content。

---

## 3. Porting 步驟(機械化,agent 可逐步執行)

### Step 1 — Inventory(列清單)
```bash
# 對既有 project 跑(假設既有在 ~/legacy_project)
cd ~/legacy_project
grep -rE "@asset|partitions_def" --include="*.py" | head -50
# 列出所有 @asset、它們的 partitions_def、deps
```
產出**清單**:`(asset_name, partition_spec, deps, compute_kind)`。

### Step 2 — 複製 scaffold
```bash
cd ~/Dagster-expert/spec_dagster
cp -r flows/_template flows/<my_flow>
cd flows/<my_flow>
sed -i 's/TEMPLATE_change_me/<my_flow>/g' spec.yaml workspace.yaml
```

### Step 3 — 寫 spec.yaml(對齊 Step 1 清單)

對照 `spec_dagster/flows/liberate_char/spec.yaml` 當範本。每個
generator/compute asset 都要:
- `name`、`kind`、`script`(`flows.<my_flow>.script:<fn_name>`)
- `partitioned_by`(維度名列表)
- `depends_on`:每個 upstream + `mapping`(`identity` / `all` / `last`
   / `{dim: identity}`)
- 對 compute:`dispatch` + `op_tags` + 若 lsf 則 `lsf:` 區塊

**新欄位**(等 framework 擴張後可用):`trigger: automation`(預設 cascade)。

### Step 4 — 寫 script.py(純函數,**絕不 import dagster**)

對每個 asset 寫對應函數:
- generator:`def gen_<name>(*partition_values) -> dict[abs_path, content]`
- compute:`def <name>_command(*partition_values) -> list[str]`(argv)

**驗證指令**(必須通過):
```bash
grep -E "^(from|import) dagster" flows/<my_flow>/script.py
# 預期:空(0 命中)
grep -E "@(asset|sensor|job)" flows/<my_flow>/script.py
# 預期:空(0 命中)
grep -wE "bsub" flows/<my_flow>/script.py
# 預期:空(0 命中)— 你的 script 不該 bsub;framework 在 dispatch:lsf 時包
```

### Step 5 — load_spec sanity check(framework 自動驗 schema)
```bash
PYTHONPATH=~/Dagster-expert/spec_dagster /tmp/dg-venv/bin/python -c "
from framework.spec.loader import load_spec
s = load_spec('flows/<my_flow>/spec.yaml')
print('flow_name:', s.flow_name)
print('assets:', [a.name for a in s.assets])
"
# 預期:列出所有 asset。若 spec 有錯,印出 Pydantic ValidationError
# 指出哪個欄位錯
```

### Step 6 — 設定 DAGSTER_HOME + 跑驗收
```tcsh
setenv DAGSTER_HOME /local/dagster_home/<my_flow>_dev
mkdir -p $DAGSTER_HOME
cp ~/Dagster-expert/spec_dagster/framework/config/dagster.localsim.yaml \
   $DAGSTER_HOME/dagster.yaml
setenv PYTHONPATH ~/Dagster-expert/spec_dagster
setenv PATH ~/Dagster-expert/spec_dagster/flows/<my_flow>/_vendor/bin:$PATH
# 啟動 daemon
/tmp/dg-venv/bin/dagster-daemon run -w flows/<my_flow>/workspace.yaml &
```

### Step 7 — 驗收(cascade 模式)

**這是新驗收方式**,要等 framework 加 `trigger: automation` 才完整可
跑。Porting agent 必須:

```bash
# materialize 最上游的 root asset(只一個指令)
/tmp/dg-venv/bin/dagster asset materialize \
  -w flows/<my_flow>/workspace.yaml \
  --select <root_asset_name>

# 然後等 daemon 的 AutomationCondition.eager() 串接整條 pipeline
# 觀察:
PYTHONPATH=$PWD /tmp/dg-venv/bin/python -c "
import dagster as dg, time
expected_leaf = '<terminal_asset>'
N = <expected_partition_count>
with dg.DagsterInstance.get() as inst:
    for _ in range(120):  # 等 20 分鐘
        n = len(inst.get_materialized_partitions(dg.AssetKey(expected_leaf)))
        print(f'  {expected_leaf}: {n}/{N}')
        if n >= N: break
        time.sleep(10)
"
# 預期:N/N(整條 pipeline cascade 跑完)
```

### Step 8 — 比對 reference 結果(determinism check)

```bash
# 對你提供的 reference 輸出做 byte-compare(過濾 path-bearing 標頭)
diff -r --ignore-matching-lines='^/\*.*source.*\*/' \
        --ignore-matching-lines='^/\*.*inputs.*\*/' \
        ~/legacy_reference_output/ /tmp/<my_flow>_dag/out/
# 預期:無差異(或只剩 path-bearing 那幾行,可接受)
```

---

## 4. 易踩雷地方(L1-L14 + Mode A cascade-specific)

### 來自 D1 build 的 14 雷(已內化於 framework,但 porting 自己的 module 仍可能踩)

| # | 症狀 | 原因 | 解法 |
|---|---|---|---|
| L1 | `DagsterInvalidDefinitionError: Cannot annotate context parameter` | 你寫 helper 用 `from __future__ import annotations` 或 `context: dg.AssetExecutionContext`(qualified) | 不用 `__future__ annotations`;`from dagster import AssetExecutionContext`,annotate 用裸名 |
| L2 | `BetaWarning: MultiToSingleDimensionPartitionMapping` 在 log | 它在 1.13.3 是 beta(behavior 對) | pin 版本;framework 邊界 suppress 即可 |
| L3 | `Definitions.get_asset_graph()` AttributeError | 1.13.3 改名 | 用 `resolve_asset_graph()` |
| L4 | `FileNotFoundError: 'dagster-daemon'` | 用 bare cmd | 用 venv 絕對路徑 |
| L6 | Headless daemon 啟動但 sensor 不發 | sensor 預設 STOPPED | framework `build_sensor` 已設 `default_status=RUNNING`;自寫 sensor 也要 |
| L9 | `get_latest_materialization_event()` 不接 partition= | 1.13.3 沒這 kwarg | 用 `get_event_records(EventRecordsFilter(asset_partitions=[pk]))` |
| L10 | `EventRecordsFilter` missing event_type | 它 positional 必填 | 永遠帶 `event_type=DagsterEventType.ASSET_MATERIALIZATION` |
| L11 | `materialization.tags['dagster/logical_version']` 是 None | 1.13.3 key 是 `dagster/data_version`(64-char) | 用對的 key |
| L13 | `instance.create_run_for_job(define_asset_job(...))` ParameterCheckError | 後者回 `UnresolvedAssetJobDefinition` | 用 `@dg.job` decorator 包 noop |
| L14 | `AttributeError: launcher._instance has no setter` | property 是 read-only | 用 `launcher.register_instance(instance)` |

### Cascade-specific 雷(新增,本驗收方式特有)

| # | 症狀 | 原因 | 解法 |
|---|---|---|---|
| C1 | materialize root 後沒 cascade,downstream 沒跑 | 你 spec.yaml 沒寫 `trigger: automation`,framework 用 reconcile sensor(要等 30s tick) | spec 加 `trigger: automation`,framework 改用 `AutomationCondition.eager()` |
| C2 | downstream cascade 觸發但 partition 不對 | `MultiToSingleDimensionPartitionMapping` 方向錯了(白皮書附錄 A item 1) | 用 framework `build_mapping`,dict-key 是 upstream、dim_name 是 downstream |
| C3 | cascade 全跑完但有 partition 沒觸 | upstream 某個 partition fail 了,Dagster `eager` 不會 cascade 到 fail 的 downstream | 看 `dagster run list` 找 fail run;修 upstream 後 re-materialize 該 partition |
| C4 | cascade 跑完但 data_version 不對 | 你的 generator script 含 path-bearing(L12) | 把 path 與 content 分離,只 hash content |

---

## 5. Mode A porting 驗收 checklist

完成 porting 後,以下全綠才算 OK:

- [ ] `framework.spec.loader.load_spec()` 退 0
- [ ] `grep "^from dagster\|^import dagster" script.py` 0 命中
- [ ] `pytest <your script tests>` 全綠(純函數)
- [ ] `dagster dev -w workspace.yaml` 啟動,UI 顯示所有 asset
- [ ] 對 root asset `dagster asset materialize --select <root>` 退 0
- [ ] daemon log 顯示 cascade(`AutomationCondition.eager` 觸發 downstream)
- [ ] 最末端 asset 達到 `N/N` 已 materialize
- [ ] reference 比對通過(diff 過濾 path-bearing 後無差異)
- [ ] 重 materialize root 兩次,第二次因 data_version 沒變,downstream **不**重跑

---

## 6. Troubleshooting

| 症狀 | 檢查 |
|---|---|
| spec load 失敗,Pydantic ValidationError | 看 error 指的欄位;對照 `framework.spec.schema.FlowSpec` 看必填項;`dispatch: lsf` 必須有 `lsf:` 區塊四欄 |
| sensor 評估但 0 RunRequests | 看 `desired - observed` 是否 = 0(reconcile 模式)或上游 data_version 沒變(cascade 模式);用 `instance.get_materialized_partitions` 比對 |
| asset body 跑了但 partition status 未推進 | 看 `materialization.tags['dagster/data_version']`,可能是 `None`(代表你 yield 的 `MaterializeResult` 沒 data_version);檢查 framework version_fn |
| LSF 沒接到 bsub | 看 `dagster.localsim.yaml` 是否 `DefaultRunLauncher`;mock bsub 是否在 `$PATH` 前段 |
| cascade 漏觸 | 看 spec.yaml 是否每個 downstream 都標 `trigger: automation`;Dagster 1.13.3 `AutomationCondition` 對 cross-location dep 行為要確認 |

---

## 7. Mode A 不要做的事(會破壞架構)

- ❌ 在 script.py 內 `import dagster`(讓 script 純函數可單測)
- ❌ 在 script.py 內 `bsub`(framework 在 dispatch:lsf 時包,你包就 nested)
- ❌ 自寫 `@asset` / `@sensor` 旁路 framework(失去 spec 唯一真相)
- ❌ 把 spec 寫在 Python 而非 YAML(失去 schema 驗證)
- ❌ 用 `define_asset_job(...)` 不 resolve 就丟給 `create_run_for_job`(L13)
- ❌ 直接寫 `launcher._instance =`(L14;用 `register_instance`)
- ❌ DAGSTER_HOME 放 NFS(SQLite 鎖死;附錄 A item 6)
- ❌ asset 用 `ins=`(會強制 IO load;framework 強制只用 `deps=`)

---

## 8. 進階:porting 半路想跳 Mode B?

如果你 porting 到一半發現「同時並發其實 >~thousands」(回頭看
`ARCHITECTURE_CHOICE.md` §3 決策樹),good news:

**spec.yaml + script.py + _vendor/ 不變**,只是把 framework 從
`spec_dagster/` 換成 `execution_fabric/`(Mode B,尚未實作)。
flow owner 端遷移 < 1 天。詳見 `PHASE_1_PLAN.md` §8 與
`PORTING_GUIDE_MODE_B.zh-TW.md`。
