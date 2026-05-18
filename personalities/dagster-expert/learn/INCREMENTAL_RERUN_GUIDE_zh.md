# 增量重跑速查指南 — Lesson 17 / 18 / 19

> 三個 lesson 是同一條主軸 (incremental change event) 的三層:
> 17 = 同 location, 18 = 跨 location, 19 = 加上 daemon 自動化.
> 本文是公司端 demo 路徑 + 坑點速查, 不是 lesson 本體.

## 三個 lesson 各自一句話

| Lesson | 一句話結論 | 試什麼 |
|---|---|---|
| 17 | 改一個 upstream partition → 只有對應 downstream partition 變 stale | data_version propagation 是否正確 |
| 18 | 同樣 contract 跨 code location 不會斷 | 多 team / 多 lib boundary |
| 19 | Daemon 自動做 17 / 18 的 rebuild | EAGER critical path 零點擊 |

跑通 19 = 自動化目標達成. 跑不通 19 → 先回去確認 17 (data_version 鏈無斷).

## 通用 5 步 drill — 每個 lesson 都長這樣

1. **Backfill 全部 partitions** → UI 全綠.
2. **改 `asset.py` 一行** — 把 `rev=1` 改成 `rev=2` 之類, 讓 payload bytes 真的不同.
3. **Reload code locations** (UI "Reload all" 或重啟 `dagster dev`).
4. **重跑 ONE upstream partition** (用 CLI 或 UI 都行).
5. **看 UI partition 熱圖** — 期待: 只有 1 (或 mapping 對到的幾個) 變黃, 其他維持綠.

**第 5 步是 lesson 的核心觀察, 不是裝飾.**

## 前置一次性設定 (每天上工先做)

```bash
# tcsh
setenv DAGSTER_HOME ~/.dagster-tutor/<NN>
# bash
export DAGSTER_HOME=~/.dagster-tutor/<NN>

echo $DAGSTER_HOME      # 必須非空
which dagster           # 必須在 ~/dagster-venv/bin
dagster --version       # 必須 1.13.3
```

每個 lesson 用獨立 `DAGSTER_HOME` (例 `~/.dagster-tutor/17`, `/18`, `/19`),
避免 instance store 互相污染.

## Lesson 17 — 三個子 lab 速跑

| 子 lab | cd 到 | 啟動 | 核心觀察 |
|---|---|---|---|
| 17a Identity | `learn/17-incremental-cross-partition/17a-identity` | `dagster dev -m identity` | 改 `ff_125c` → 只 `ff_125c` stale |
| 17b Static-mapping | `learn/17-incremental-cross-partition/17b-static-mapping` | `dagster dev -m staticmap` | 改 `root` → `lvf/em/ht` 全 stale (fan-out); 改 `ff` → 只 `fast_group` stale (routing) |
| 17c constant-hash trap | `learn/17-incremental-cross-partition/17c-data-version-trap` | `dagster dev -m trap` | 改 raw → mid 顯示 stale, 但跑完 mid 後 **final 永遠 fresh** |

**17c 最重要**: 你會親眼在 UI Materializations tab 看到兩次 `Data version` 欄位值**相同** —
這就是 production bug. 真實 char flow 若中間任一步 hash 是常數, 整條鏈就斷.

## Lesson 18 — 跨 location

```bash
cd learn/18-cross-location-staleness
dagster dev -w workspace.yaml
```

兩個 location (`lib_lower` / `lib_upper`), 同 4 branch partition.
改 `lower/asset.py` 一行 → reload → 重跑 `lib_lower/kit_summary` 的 `corner` 一個 partition →
看 `lib_upper/signoff_report` 的 `corner` 變 stale, 其他 3 個還綠.

**唯一陷阱**: `lib_upper` 的 `Definitions(assets=[...])` 不可放 upstream 的 `AssetSpec` —
這是 Day-7 federation bug (`Error loading base asset job`). 本 lesson 已避開.

## Lesson 19 — Daemon 自動補做

```bash
cd learn/19-auto-materialize-partitioned
dagster dev -m reactive
```

**`dagster dev` 同時啟 webserver + daemon. 缺一個 auto-mat 就不會 fire.**
看啟動 log 確認 `Started Dagster daemon process` 字樣.

UI 左側 "Auto-materialize" sidebar → 確認 daemon "running" + policy "enabled".

Demo 流程:
1. Backfill `raw_corner` 4 partitions → 等 ~30s → 看 `mid_corner_eager` / `final_corner_eager` 自動跑完 8 個 partition.
2. `final_corner_lazy` **不會自動跑** (LAZY 行為正確).
3. 改 `raw_corner` payload `rev=1`→`rev=2` → reload → 重跑 `ff_125c` 1 個 partition.
4. 等 ~30s → daemon 自動補 `mid_corner_eager[ff_125c]` 和 `final_corner_eager[ff_125c]`. 其他 3 個 partition 不動.
5. 完成. 整條 EAGER chain 增量更新, 零點擊.

## 速查: 看到這個症狀就查這個

| 症狀 | 多半是 | 怎麼確認 |
|---|---|---|
| 改 upstream 一個 partition, downstream **全部** 變 stale | data_version 是常數 (17c 的 trap), 每個 partition 看起來都一樣不動 | UI Materializations tab 看 `Data version` 欄是否真的隨 partition 變 |
| 改 upstream + reload, downstream **完全沒** 變 stale | **還沒 re-materialize upstream** (reload 只是讀新 code, 不會自動觸發 stale) | 跑 5 步 drill 的 step 4 (`dagster asset materialize --select <upstream> --partition <key>`); 確認 `payload = ...` 真的改了 |
| 19 daemon 不動 | daemon 沒跑 / policy 沒 enable | `dagster dev` log 找 daemon process; UI sidebar 確認 |
| 19 daemon 一直在重跑同一個 partition | data_version 含 timestamp 等非決定性內容 | grep `time.time()`, `uuid`, `datetime.now()` 等 |
| 17b 跑不起來, 報 `mapping target partitions not in...` | `StaticPartitionMapping` value list 比 downstream 的 partition keys 多 | filter mapping values 到 downstream 真的有的 keys (PR #7 同一坑) |
| 18 報 `Error loading base asset job` | `lib_upper` 多放了一個 `AssetSpec` | 確認 `Definitions(assets=...)` 只有自家 asset |

> **Reload 與 stale 的關係 (1.13.3 機械規則)**: reload 本身**不會**觸發 stale。
> stale 只有兩個觸發點:
> 1. **re-materialize upstream** → 寫入新的 `data_version` → 下游 stale
> 2. **顯式 bump `@asset(code_version="N")` → `"N+1"` + reload** → 立即 stale, 不用 materialize
>
> 1.13.3 **不會**自動從 source 算 code_version。本指南涵蓋的 17/18/19 全部都沒設 `code_version=`,
> 所以下游 stale 的唯一觸發點是 5 步 drill 的 step 4 (`re-materialize upstream`).
> 詳見 `database/dagster-1.13.3/docs/data-version-and-staleness.md` § "What reload does".

## 帶到公司之前的自我檢查

- [ ] 三個 lesson 在家機都 `dagster definitions validate` 過? (PR #11 已做)
- [ ] `~/dagster-venv` 1.13.3 + `dagster-webserver`/`dagster-daemon` 都裝齊?
- [ ] 公司端 `DAGSTER_HOME` 路徑想好 (例 `/var/lib/dagster-demo/<NN>`)?
- [ ] 公司端 port 3000 沒被佔 (預設; 或想好換 `-p`)?
- [ ] 預期受眾: 自己驗證 / 給 team 看 / 給主管看? 後兩者建議只 demo 19 + 17c (最有戲).

## 真實 AP 流的對應

| Lesson | 對應 production 場景 |
|---|---|
| 17 | 單 lib 內 21 step × 46 branch 的 step→step 增量 |
| 18 | apl / pgv / cdk 多個 kit 跨 lib 邊界 (multi-team) |
| 19 | TSMC AP 自動化目標 — upstream char output 一變, 下游 sign-off 自動跑對的 branch |

`demo/scale-lib/` 是 1+2 的 production-shaped reference;
3 還沒接上去 (因為公司端 daemon / scheduler 還沒到位). 未來補上, 接 19 的 EAGER policy 即可.

## 不在這份指南內的東西

- 客製 `AutoMaterializeRule` (例 off-hours skip) — 1.13.3 有 API, 但 90% 場景 `eager()`/`lazy()` 夠用.
- `AutomationCondition` — 1.14+ 才有, air-gap 暫不適用.
- 跨 partition × 跨 location × auto-mat **三合一** — 把 18 的 downstream 加 `AutoMaterializePolicy.eager()` 即可, 留給你自己組合.
