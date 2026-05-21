# lesson 20 · multi-library grain (生產級規模)

**Time**: 90-120 min · **Prerequisites**: lessons 10 (branched flow), 11 (multi-library), 17/18/19 (incremental / staleness / auto-materialize)

> 中文先,英文 inline 補充。

---

## 為什麼有這個 lesson

跟主管討論完內部 AP characterization 的實際情況後,**asset 切分尺度改為**:

| 過去 (lesson 11 / scale-lib demo) | 現在 (lesson 20) |
|---|---|
| library 用 `key_prefix`,group_name = `library_branch` 混合 | **library = `group_name`** (一個 library 一個 UI group) |
| asset 是 `branch_step` 名稱組合 | **asset = step / kit** (純 step 名,不混 branch) |
| branch / step 分散在 asset key 中 | **branch = partition** (每個 asset 共用同一個 partition 軸) |
| PVT / cell 在 partition 第二軸 或 source observable | **PVT / cell 完全不在這層** (留給 fine tier nested Dagster) |

換句話說,Tier-1 Dagster 只管 **「grain」** — library × step × branch。fine (PVT / cell) 在 partition 觸發後由 asset body 內部 spawn 一個獨立 Dagster 處理 (lesson 20 不示範這層;這裡只 stub asset body)。

### 跟 scale-lib demo 的關係

scale-lib (在 `demo/scale-lib/`) 是 **單 library** 的 reference,有 4-layer import 強制 + folder_digest + perl/python runner + 89 test。

lesson 20 把 scale-lib 的核心 (parent-mirror partition mapping + step taxonomy + branch hierarchy) 蒸餾到 **lesson 規模**,並加上:

- 100 library (production-shape)
- group_name = library 純單值 (新的 UI grouping)
- asset body 是 stub (不背 runner 複雜度)
- 不提 PVT / cell (留給 fine tier)

當你要真的接 perl/python runner 跟 LSF,**回去看 `demo/scale-lib/pipelines/runners.py`**;這層 lesson 20 不重複。

---

## 設計三大決定 (Cardinality 算給看)

### 1. 100 library = 100 group

```
asset count = 100 libraries × 21 steps = 2,100 個 @asset
group count = 100 (group_name 就是 library 名)
```

UI 在 lineage view 預設展開 2,100 個 node 是不可看的。但 **Groups 視圖** 把每個 library 收成一個 bubble,**你只會看到 100 個 bubble**,點開要看的 library 才展開。

> 跑 `python3 cardinality_calc.py` 先看完整數字。

### 2. branch = partition (46 個 static partition key)

每個 asset 的 `partitions_def` 是 46 branch 的 `StaticPartitionsDefinition`,**除了** SETUP_ROOT_ONLY 跟 KIT_ROOT_ONLY 兩類 step 只有 1 個 partition (`corner`)。

```
partition records:
  root-only steps (2 setup + 9 kits) × 100 lib × 1 branch  = 1,100
  all-branch  steps (4 extract + 6 char) × 100 lib × 46    = 46,000
  ─────────────────────────────────────────────────────────────────
  total                                                    ≈ 47,100
```

47K 筆 partition record SQLite 跑得動。要 concurrent backfill 多開幾條線就上 Postgres。

### 3. Dep 模型 (跟 scale-lib 一致,full parent-mirror)

| 規則 | 適用 step | upstream | partition mapping |
|---|---|---|---|
| **SetupGate** | 所有非 setup step | `step0` | `SpecificPartitions(["corner"])` |
| **StepChain** | chain step (step3..step6) | 前一個 chain step | `IdentityPartitionMapping` (same-branch) |
| **ParentMirror** (★) | step5 (預設) | step4 | `StaticPartitionMapping`: 每個 downstream branch `b` 同時讀 `step4[b]` + `step4[parent_of(b)]` |
| **Step7Follow** | `step7` | `step1` | `IdentityPartitionMapping` |
| **KitStep6Gate** | kits 除 `rln` 外 | `step6` | `SpecificPartitions(["corner"])` |

**「同 step branch 完全平行」** → 沒有 branch 之間互相 block,因為每個 partition 各自獨立 (IdentityPartitionMapping 預設行為)。

**「跨 step 同名 branch 達成就往下」** → 由 IdentityPartitionMapping 自動處理,`step3[lvf]` 只等 `step2[lvf]`,不等 `step2[corner]`。

**parent-mirror 是上面規則的補強**:在 step5 這個 char merge 點,非 root 的 branch 額外讀 root (或近親) 的同 step 結果。對應 EDA 內部 `corner` merge 步驟的常見需求。

---

## 跑

### 0. 環境 (tcsh)

```tcsh
# 每個 lesson 都要 isolation,別共用 ~/.dagster-tutor 根目錄
setenv DAGSTER_HOME ~/.dagster-tutor/20-multi-library-grain
mkdir -p $DAGSTER_HOME

# (bash: export DAGSTER_HOME=~/.dagster-tutor/20-multi-library-grain)

# 確認 venv 跟 dagster 1.13.3
source ~/dagster-venv/bin/activate.csh   # (bash: source .../activate)
which dagster
dagster --version          # 應顯示 1.13.3
```

### 1. Cardinality check (不需要 Dagster)

```tcsh
python3 cardinality_calc.py
```

預期輸出開頭:
```
Libraries (= UI groups)         : 100
Steps per library (= assets/lib): 21
Branches                        : 46 (root: 1)

Total assets                    : 2,100
Total partition records         : 47,100
```

### 2. Smoke (in-process,3 library × 7 step × 5 branch)

```tcsh
python3 _smoke.py
```

預期 < 1 min (實測 in-process ~5s)。最後一行 `SMOKE PASS — 96 successful materializations across 3 libraries.` (= 3 lib × (1 step0 + 5 step1-branch + 25 chain-step5branch + 1 meta) = 3×32)

### 3. Dagit (互動式)

```tcsh
dagster dev -w /abs/path/to/learn/20-multi-library-grain/workspace.yaml
# 瀏覽器開 http://127.0.0.1:3000
```

**首次 load Definitions 會花 5-15 秒** — 在 build 2,100 個 @asset。後續互動就快了。

---

## UI 導覽 (重點驗證項目)

進到 Assets → Lineage 之後切換 **Groups view** (左上 toggle)。應該看到:

1. **100 個 bubble**,每個對應一個 library (svt_p1_h6_075 .. ulvthp_p2_h9_090)
2. 點任一 bubble 展開 → 看到該 library 的 21 個 asset (step0 → meta) 跟 dep 線
3. **Partition matrix** (任一 asset 右上角) → 顯示 46 個 branch 的 partition 狀態
4. **Step5 的 dep view** → 你應該看到 step5 的入線是 (step0[corner] + step4 with StaticPartitionMapping 的 parent-mirror 行為);UI 會顯示 partition mapping 為 `StaticPartitionMapping`

### 驗證可看性

問自己幾個 production-realistic 的問題:

- 找 `svt_p1_h6_080` library 的 step5 — 幾秒能找到?
- 看到 step5 跟 step4 的 partition mapping,點 partition matrix 看 `step5[lvf]` 它的 upstream 是 `step4[lvf]` + `step4[corner]` 嗎?
- 同時 backfill 3 個 library — 用 GraphQL `launchPartitionBackfill` 各送一個 backfill 帶 `partition_keys`,看 dagit run 頁面有沒有 saturated。

如果 100 group 在 dagit 太擠,可以把 `pipelines/libraries.py` 的 `_enumerate()[:100]` 改成 `[:20]` 先壓力測試小一點。

---

## Bridge 到真實 AP — 要改哪些

| 改哪 | 改成什麼 |
|---|---|
| `pipelines/libraries.py` `LIBRARIES` | 真實的 library 名 (從你內部 inventory 讀檔) |
| `pipelines/factory.py` `_impl` body | `subprocess.run([perl_or_python, script, --library, --branch, --step, --out])`,或進一步 `bsub -K ...` |
| `pipelines/factory.py` `MaterializeResult.data_version` | 用 `folder_digest.digest_folder_manifest(out_dir)` 取代 stub string (借用 `demo/scale-lib/pipelines/folder_digest.py`) |
| Tier 2 (PVT / cell) | asset body 內 spawn 一個獨立 Dagster 跑 fine-grain;ephemeral `DAGSTER_HOME` 在 scratch path |

scale-lib demo 的 4-layer 架構 (`pipelines/spec/` ↔ `pipelines/rules/` ↔ `registry.py` ↔ `translator.py` ↔ `factory.py`) 是當 dep 規則複雜到需要多個 rule 互相疊加時的工程化模式 — 這個 lesson 為了 readability 把全部疊在 `edges.py` 一支裡。production 規模再考慮拆分。

---

## 常見 gotcha (1.13.3)

| Symptom | Cause | Fix |
|---|---|---|
| `DagsterInvalidDefinitionError: type annotation` 在 factory.py | `from __future__ import annotations` 讓 context 變 string | factory.py 不能有 `__future__ annotations` (其他檔案可以) |
| Partition matrix 顯示 step5 的 upstream 是 all-partitions 不是 parent-mirror | `IdentityPartitionMapping` 或 `StaticPartitionMapping` 沒生效,通常是兩個 asset 的 `partitions_def` 是不同物件 | `pipelines/partitions.py` 必須 module-level singleton,所有 asset 共用同一個 def 物件 |
| `validate_partition_mapping: target partitions not in downstream partitions definition` | 對 root-only downstream 用了 all-branch 的 StaticPartitionMapping | 用 `SpecificPartitionsPartitionMapping(["corner"])`,別用 `StaticPartitionMapping` |
| Definitions load 超過 30 秒 | 100 library × 21 step × 46 branch 觸發 Dagster asset-graph N² 驗證 | 確認版本 1.13.3 (不是更舊的 1.10);考慮分 code location |

---

## Now-try

1. **把 LIBRARIES 縮成 6 個** (改 `pipelines/libraries.py`),smoke 跑全部 21 step × 全 46 branch (改 `_smoke.py` 的 `branches_to_test`)。比較 dagit lineage 跟 100 lib 時的 navigability 差異。
2. **加一個 cross-library dep**: 例如 `ulvthp_*` 都依賴 `svt_p1_h6_075` 的 `meta`。在 `pipelines/edges.py` 加一個專用 rule;`AssetDep(asset=AssetKey(["svt_p1_h6_075", "meta"]), ...)`。看 dagit 跨 group 連線顯示。
3. **把 step5 的 parent-mirror 也套到 step4**: `pipelines/steps.py` 改 `DEFAULT_PARENT_MIRROR_STEPS = frozenset({"step4", "step5"})`,重 load,看 partition matrix 變化。

---

## Cardinality memorize

```
this lesson           : 100 lib × 21 step × 46 branch ≈ 2.1k asset / 47k partition record
single library prod   :   1 lib × 21 step × 46 branch ≈   21 asset / 1.1k partition record  (= scale-lib demo)
huge library scale-up : 100 lib × 21 step × 64 branch ≈ 2.1k asset / 65k partition record
```

SQLite 在 ~100k partition record 都還健康;Postgres 留給 concurrent backfill 或更高量級。
