# EDA Characterization Orchestration — Architecture White Paper
> **文件目的**：本白皮書描述一套以 Dagster 為資料身分層、自建執行層為計算引擎的 EDA characterization 編排系統。文件分兩階段（Phase 1 快速落地、Phase 2 完整架構），並明確對比「原 blocking + LSF Run Launcher 方案」與「現方案」的差異與取捨。
>
> **讀者**：接手實作或修改本設計的工程師 / AI agent。文件刻意記錄「為什麼這樣設計」（rationale），而不只是「設計是什麼」，以避免接手者在不理解取捨的情況下退回已被否決的方案。
>
> **環境前提**：air-gapped、LSF job scheduler、NFS 共享儲存、可用 Kafka（無 RabbitMQ）、PostgreSQL 可部署於專用主機、tcsh/csh 環境。
>
> **與 v1 白皮書的關係**：本文件**取代** `FIVE_LAYER_WHITEPAPER.md`(v1)。v1 把 Dagster 同時當 lineage 與 execution layer(自寫 `LSFRunLauncher` 把 run worker 丟上 LSF);本文件(v2)把這兩個職責徹底分開,Dagster 只剩 lineage。v1 在 §2 被作為「被否決方案」分析(其優缺點誠實記錄),作為決策歷史保留。對應的實作計畫見 `PHASE_1_PLAN.md`。
---
## 0. TL;DR
- **核心分層**：把「資料身分」（lineage / data version / 增量判斷）與「執行過程」（dispatch / 排隊 / 實際計算）徹底分開。Dagster 只負責前者；自建的執行層負責後者。
- **關鍵心智模型**：Dagster 的 **run 執行追蹤**（process 活著嗎、跑到哪)要繞過；Dagster 的 **asset/lineage 追蹤**（算到什麼 version、依賴關係)要牢牢保留。兩者解耦。
- **與原方案的根本差異**：原方案讓「一個 LSF job = 一個 Dagster run」，run worker 的生命週期等於計算時長,導致 coordinator/daemon 在大量、長時計算下成為瓶頸。現方案讓 Dagster run 只負責「投遞」與「收割」，實際計算下放到 Dagster 看不見的執行層。
- **兩階段**：Phase 1 不做 message queue,直接用 LSF run client 撒 job + SQLite 記狀態（含 file lock)，先驗證資料身分層與收割機制；Phase 2 補上完整 message queue（Kafka）+ worker pool + PostgreSQL + 故障修復。
---
## 1. 背景與問題定義
### 1.1 工作負載特性
- 任務單位：cell-level characterization（單份計算可能數十分鐘至小時級)。
- 規模：單批可達 10k+ 份計算；後續迭代常常只有約 1% 需要重跑。
- 痛點核心：**避免重複計算**。當少量上游變更時,必須只重算「受影響的下游」，而非全部重跑。這是整個系統存在的理由。
- LSF 排隊現實：job 從 `bsub` 到真正開始執行（PEND → RUN），**通常數分鐘,塞車時可達數十分鐘至小時級**。此數字對狀態設計有決定性影響（見 §5)。
### 1.2 為什麼需要 lineage（為什麼是 Dagster)
「1% 增量重跑」這個需求,本質上需要一個能表達任務間依賴、並能依「上游資料版本是否改變」判斷下游是否需重算的系統。Dagster 的 asset 模型 + `DataVersion` 機制天生勝任此事。這是選用 Dagster 的**唯一理由**——不是因為它的 queue、也不是因為它的執行能力,而是它的 data-aware lineage。
> **給接手者的警告**：任何「為了簡化而丟掉 lineage」的提議,等於拆掉本系統存在的根基。lineage 是核心資產,不是裝飾。
---
## 2. 原方案（Blocking + LSF Run Launcher）回顧與否決理由
### 2.1 原方案描述
- 自訂 `LSFRunLauncher`：把**整個 Dagster run** 透過 `bsub` 丟到 LSF 節點執行。
- run worker 在 LSF 節點上啟動,內部用 `subprocess` 直接跑 characterization script（因為已經在 LSF 上)。
- 計算結束後就地計算 DataVersion,透過 Dagster 原生 materialization 機制讓 sensor 辨認狀態。
### 2.2 原方案的優點（誠實記錄,避免接手者以為它一無是處)
- **失敗語意天然正確**：run 的成敗 = 計算的成敗,Dagster 原生掌握。re-execution、run_retries、from-failure、UI 狀態全部自動正確。
- **架構簡單**：順著 Dagster 的設計走,不需自建任何外部狀態機。
- **與 Dagster 心智模型契合**：適合推廣給內部其他不熟悉系統的使用者。
### 2.3 否決理由：資源模型在本工作負載下不可擴展
原方案的根本問題是 **run worker 的生命週期 = 計算時長**：
- 每一份正在跑的計算,都需要一個活著的 run worker process 對應。計算跑一小時,run worker 就佔一小時。
- 10k 份長計算 = 需要同時存在 10k 個 run worker。即使用 LSFRunLauncher 把它們丟到 LSF,那也是 10k 個「協調殼 + 計算」一起佔用 LSF 資源。
- **coordinator / daemon 的 dequeue 吞吐**成為瓶頸：daemon 是「逐個 push `launch_run`」的模型,在「同時數千個長 run」時,啟動吞吐會成為序列瓶頸（實測案例：設定允許 512 並行,實際只跑起 60–80 個)。
> **關鍵區分**：原方案的瓶頸**不是** Dagster「裝不下 10k 個待辦」（queue 存量幾乎無限)。瓶頸是「同時**執行中**的 run worker 數量」受物理資源限制,以及「daemon 逐個推送啟動」的吞吐限制。現代框架處理百萬級待辦靠的是 **pull-based**（worker 主動拉取)或讓底層排程器（K8s/LSF array）承擔 dispatch；Dagster 的 daemon 不是為「同時數萬個獨立 run」最佳化的。
### 2.4 從原方案到現方案的演進階梯
```
blocking（原方案)
  run worker 生命週期 = 計算時長 → run 數 = 計算數 → daemon 啟動瓶頸最痛
     ↓ 解耦「等待」
non-blocking
  run 發 bsub 後不阻塞 → run 短命 → 但每份計算仍需一個 run 去 bsub（dispatch 瓶頸緩解但仍在)
     ↓ 解耦「dispatch」
pull-based（現方案 Phase 2 目標)
  Dagster run 只投遞任務到自建執行層 → worker pool 自取 → 一個 run 可投遞海量任務
  Dagster 退化為「決定算什麼 + 收割結果」，執行完全在它看不見的執行層
```
每一步都在「往下游推一個責任」：blocking 把所有責任壓在 run；non-blocking 把「等待」推給 LSF；pull-based 再把「dispatch」也推給執行層。Dagster 的角色一路退到「投遞者 + 身分記錄者」——這正是它該待的位置。
---
## 3. 目標架構：三領域模型
整套系統分為三個領域,各有單一職責,並透過明確定義的「跨界線」資料流互動。
### 3.1 領域一:資料身分（Dagster)
**鐵律**：只回答「該算什麼」與「算到什麼版本」，從不回答「現在算到哪、worker 活著嗎」。
| 模組 | 職責 | 關鍵約束 |
|---|---|---|
| Asset graph | 用 asset 依賴表達 lineage；`DataVersion` 判斷哪些下游受影響需重算 | data version 是「避免重複計算」的唯一依據——上游 version 沒變,下游根本不投遞 |
| Request 打包（投遞 run) | 短命的 run：不執行 script,而是把「需要算的 request」投遞到執行層,然後立即結束釋放 | **絕不在此 blocking 等計算**,否則退回 blocking 模式的吞吐瓶頸 |
| Sensor（收割） | 讀取執行層的狀態,把完成的 request 回填成對應 asset 的 `MaterializeResult`（帶 data version) | **唯一縫合點,且必須冪等**（見 §6 故障處理) |
> **用語對齊**：本文件統一用「request」指涉一個待執行的計算單位（對齊 Dagster 的 run request 用語)。一個 request 對應一份 characterization 計算。
**Dagster 元件配置**：
- RunLauncher：`DefaultRunLauncher`（投遞/收割 run 都很輕,跑在本機)。
- Run coordinator：`QueuedRunCoordinator`，`max_concurrent_runs` 可設小（這裡只有少量投遞/收割 run,真正的大量在執行層)。
- Run storage：PostgreSQL（**絕不用 NFS 上的 SQLite** 當 Dagster backend——已知會因多 process 競爭導致 alembic 衝突與分鐘級 sensor tick)。
- 僅啟動 daemon,**不啟動 Dagster UI**（理由見 §3.3)。
### 3.2 領域二:執行引擎（Execution Fabric)
**鐵律**：Dagster 在此完全失明。此層只透過兩個介面與外界接觸——queue 的「投遞入口」與狀態 DB 的「結果出口」。
| 模組 | 職責 | 關鍵約束 |
|---|---|---|
| Task 佇列（priority queue) | 存放待執行 request；worker 主動 pull 認領 | 每筆 request 帶冪等鍵；priority 支援孤兒高優先重排 |
| Worker pool | 常駐 worker 迴圈：認領 → bsub 到 LSF → 算 data version → 寫狀態 → 再認領 | 計算成功與寫入必須原子綁定（`set -e; set -o pipefail`) |
| 狀態 DB | task 的當前執行狀態（執行的真相來源) | 單一 writer（只有 worker pool 寫)；嚴格狀態機 |
| LSF 狀態同步者 | 持續 `bjobs` 監看 LSF job,維護 SUBMITTED ↔ RUNNING 轉換 | **必需模組**（理由見 §5)；維持 worker 非阻塞 |
> **命名說明**：此層在討論中曾稱「pull-based queue」，建議正式命名為 **Execution Fabric**（執行織體),因為它是 queue + worker pool + 狀態回報三者構成的執行底層,而非單一佇列。
### 3.3 領域三:Control + UI
**鐵律**：只讀聚合 + 觸發入口,**永不持有自己的狀態真相**（避免出現第三個 source of truth)。
| 模組 | 職責 | 讀/寫關係 |
|---|---|---|
| Control logic | 決策中樞：(a) 決定重跑哪些；(b) 調整 worker pool 大小 | 與 UI 雙向；**對 status table 只讀**；對 event/resource control 單向 |
| Event control | 指向 asset graph,控制重跑（改變上游 data version 觸發 lineage 重算) | 單向 → Dagster |
| Resource control | 指向 worker pool,控制 pool 設定（worker 數量動態調整) | 單向 → worker pool |
| Status table | 被 worker pool 寫、被 sensor 讀、被 control logic 讀 | 單一 writer（worker) |
| UI | 聚合呈現：從 Dagster 讀 lineage 進度,從狀態 DB 讀執行進度 | 只讀兩邊；與 control logic 雙向 |
> **為什麼拋棄 Dagster 原生 UI**：投遞 run 在 Dagster UI 上會很快變「綠燈成功」（因為它只負責投遞),但真正的計算還在執行層跑。這會造成「綠燈但其實還在 run」的認知陷阱。自建 UI 直接讀狀態 DB,呈現的是執行真相,不是投遞真相。
> **重要修正（已採納)**：Control logic 對 status table 改為**只讀**。這使 status table 回到單一 writer（只有 worker pool),消除多 writer 競態。Control 的控制動作透過 event control / resource control 對「被控對象」下達,而非直接寫 status table。
### 3.4 跨界線資料流（整個架構的命門)
```
Control ──(event control)──> Dagster asset graph
                                   │ lineage 判斷哪些需算（上游 version 沒變則不投遞)
                                   ▼
                            投遞 run（短命)
                                   │ 投遞 request（廉價,一次可投海量)
                                   ▼  ← 跨界線：向下穿過,Dagster 放手
                            Execution Fabric（Dagster 失明)
                              Task queue → Worker pool → bsub → LSF
                                   │ worker 算完,原子寫入
                                   ▼
                            Status DB（執行真相)
                                   │ 收割 sensor 讀取
                                   ▲  ← 跨界線：向上穿回,Dagster 重新接管身分
                            收割 sensor 回填 MaterializeResult（冪等)
                                   │
                                   ▼
                            Dagster lineage 推進（身分真相更新)
Control ──(resource control)──> Worker pool（動態調整 worker 數)
Status DB ──(只讀)──> Control logic / Sensor / UI
```
**最關鍵的兩條線**：
1. **投遞線（向下)**：request 穿過邊界進入執行層時,Dagster 放手——不追蹤它的執行。
2. **收割線（向上)**：結果穿回邊界時,Dagster 才重新接管它的身分。收割 sensor 是「執行真相 → 身分真相」的**唯一交棒點**,整個系統的正確性壓在它的冪等性上。
### 3.5 Source of Truth 主從關係（必須嚴格遵守)
| 真相類型 | 權威來源 | 衍生方 |
|---|---|---|
| 執行狀態（跑了沒、結果、data version) | **Status DB**（worker 寫) | — |
| 身分 / lineage（算到什麼版本、依賴推進) | **Dagster materialization** | 衍生自收割 sensor 從 Status DB 讀到的結果 |
| 控制決策 | Control logic（不落地狀態,只下指令) | — |
收割 sensor 是唯一把「執行真相」轉成「身分真相」的縫合點,此轉換必須冪等、可重試。
---
## 4. 狀態機設計
### 4.1 狀態定義（含 LSF 兩階段拆分)
關鍵洞察：「RUNNING」對 request 是「被 worker 認領了」,但對實際計算,worker 可能只是把 job `bsub` 到 LSF、**正在等 CPU**,根本還沒真正算。必須拆分,否則無法區分「worker 不夠」與「LSF slot 不夠」兩種瓶頸。
```
PENDING    ── 在 queue 中,等待 worker 認領
  ↓ worker 認領並 bsub 到 LSF
SUBMITTED  ── worker 已認領、已送 LSF,但 LSF 上在等 CPU（尚未真正執行)
  ↓ LSF 開始實際執行（bjobs: PEND → RUN)
RUNNING    ── LSF 節點上真正在算
  ↓ 計算結束
SUCCESS / FAILED   ── 終態,sensor 可收割
（預留)PENDING/SUBMITTED ──Control 取消──> CANCELLED
（例外逆向)SUBMITTED/RUNNING ──[孤兒]──> reaper 移除 + 高 priority 重排 → PENDING
```
### 4.2 嚴格單向推進
- 用條件 update（樂觀鎖)強制合法轉換。例如 worker 認領：
  ```sql
  UPDATE status SET state='SUBMITTED', worker_id=?, lease_expires=?
  WHERE idempotency_key=? AND state='PENDING'
  -- 影響 0 行 → 已被別的 worker 搶走,放棄
  ```
- 既然 status table 單一 writer（worker)+ 單一逆向操作者（reaper),狀態機維護單純：只有 worker 推進正向,只有 reaper 做孤兒重置。
- **CANCELLED 終態**：Phase 1/2 可暫不實作,但狀態機設計時留位子,避免日後硬塞。
### 4.3 為什麼 SUBMITTED / RUNNING 拆分至關重要
- 大量 SUBMITTED + 少量 RUNNING → **LSF slot 不足**（解法：要更多 LSF 配額,或降低投遞速率)。
- 大量 PENDING + 少量 SUBMITTED → **worker 不足**（解法：resource control 加 worker)。
- 若不拆分,只會看到「一堆 RUNNING 但沒進度」，完全無從判斷該加 LSF 還是加 worker。在數小時排隊的情境下,此診斷能力是運維生死線。
---
## 5. LSF 排隊時長的設計影響（數分鐘 ~ 小時級)
LSF job 從 `bsub` 到真正執行可能停留數小時。這使 SUBMITTED 不是短暫過渡態,而是 request 生命中可能停留最久的狀態。後果：
### 5.1 SUBMITTED 的孤兒判斷必須用 bjobs,不能用時間
```
SUBMITTED 階段「卡住」判斷：
  不問「等多久了」，而問 bjobs：這個 LSF job 還在不在？
    在、PEND  → 正常排隊,繼續等（即使 3 小時也健康)，不重排
    在、RUN   → 已開始跑,狀態本該推進到 RUNNING（同步漏了,補上)
    不在（消失)→ 真孤兒：bsub 失敗或 job 被殺 → 重排
```
**健康定義翻轉**：SUBMITTED 的健康不是「沒超時」，而是「LSF 確認它還在隊伍裡」。用時間閾值會把正常排隊的 task 誤判成孤兒、重複 bsub,把 LSF 隊伍推進惡性循環。
### 5.2 存活證明來源分階段
| 階段 | 誰在做事 | 存活證明來源 |
|---|---|---|
| PENDING | 沒人（在 queue 等) | 不需要 |
| SUBMITTED | LSF（排隊中) | **bjobs 查 LSF** |
| RUNNING | LSF 節點（真正算) | bjobs（可選加計算 heartbeat) |
worker 一旦 bsub 完,其生死與 task 解耦——task 命運交給 LSF,由 LSF 狀態同步者透過 bjobs 監看。因此 **LSF 狀態同步者是必需模組,不是可選**。
### 5.3 投遞改為節流（重要策略)
不要一次把所有 request bsub 進 LSF（會讓 LSF 隊伍爆炸,且失去調度控制)。改為：
- queue 當緩衝,堆放 PENDING request。
- worker 控制 bsub 進 LSF 的速率,維持「LSF 上同時的 SUBMITTED + RUNNING」在合理深度（對齊 LSF 配額)。
- **好處**：priority 重排在自己的 queue 層有效（還沒 bsub 的可重排序),而非把調度權拱手給 LSF 排程器。呼應「不讓 LSF 做 schedule、保留彈性」的判斷。
---
## 6. 故障處理（Phase 2;Phase 1 暫不實作)
三種故障處理,全靠同一把 **冪等鍵** `(asset_key, partition, 觸發指紋)`。「觸發指紋」應包含上游 data version 的 hash,使「上游沒變的重複觸發」被去重,但「上游真的變了的重算」因指紋不同被視為新 request。
### 6.1 故障一:投遞去重（生命線入口)
- **場景**：同一 request 被投進 queue 兩次（sensor 重觸發 / Control 重複下令)。
- **負責**：Task 佇列（投遞時把關)。
- **機制**：投遞用冪等鍵做 upsert,不做 blind insert。
  ```
  以冪等鍵為唯一鍵：
    queue 中已存在同鍵的 PENDING/SUBMITTED/RUNNING → 不重投（no-op)
    不存在、或同鍵前一個已 SUCCESS/FAILED → 才投遞
  ```
- **不變量**：任一時刻,同一冪等鍵在 queue 中最多一個 active（PENDING/SUBMITTED/RUNNING）request。
### 6.2 故障二:孤兒 request 回收（生命線中段)
- **場景**：worker 認領後自己掛了（當機 / 搶占 / OOM),request 停在 SUBMITTED 或 RUNNING,永遠不會完成、也沒人重投。
- **負責**：reaper（建議放在 Control logic 職責下,或獨立輕量 daemon)。
- **機制**：lease + bjobs 確認（**非純時間**)。
  ```
  reaper 週期性掃描 SUBMITTED/RUNNING：
    SUBMITTED：用 bjobs 確認 → 消失才重排（在 LSF 正常 PEND 不重排)
    RUNNING  ：bjobs 消失,或執行超過單 task 最長時間 → 重排
  重排動作 = 移除舊記錄 + 以高 priority 重新投入 queue（冪等鍵不變)
  重置次數上限 N：超過則標記 FAILED（避免毒丸 task 反覆殺 worker)
  ```
- **不變量**：任何進入 SUBMITTED/RUNNING 的 request,最終一定到達終態,不永久卡住。
- **設計要點**：
  - 「移除 + 重排」必須原子（或先確實移除舊記錄、鎖住該鍵、再投新的),否則短暫同鍵兩筆會觸發故障一的去重誤判。
  - 冪等鍵不變 → Dagster 視為同一邏輯 task,收割不產生重複 materialization。
### 6.3 故障三:收割冪等與失敗恢復（生命線出口,最脆弱)
- **場景**：sensor 讀 SUCCESS 回填 Dagster 時：(a) 中途掛了重啟 → 重複回填;(b) 回填了但 cursor 沒更新 → 下次又回填;(c) 漏掉某些 SUCCESS → lineage 永不推進。任一情況導致「執行真相」與「身分真相」不一致,直接破壞避免重複計算的正確性。
- **負責**：收割 sensor（系統唯一真相交棒點)。
- **機制**：cursor + 冪等回填。
  ```
  sensor 每 tick：
    從 cursor 位置讀 status 表中 SUCCESS 且尚未收割的 request
    對每個：以冪等鍵回填 MaterializeResult（重複回填同鍵 → 覆寫為相同結果,無副作用)
    先完成回填、再推進 cursor
  ```
- **不變量**：同一 request 的完成,無論 sensor 看到幾次、重啟幾次,在 Dagster 中只產生一次正確 materialization;且所有 SUCCESS 最終一定被收割（不漏)。
- **設計要點**：
  1. **冪等優先於精確一次**：不追求 exactly-once（分散式下極難),設計成「重複收割無害」+ at-least-once。
  2. **cursor 順序**：先回填、再推進 cursor。寧可 cursor 落後（導致重讀,冪等無害),不可超前（導致漏收割)。
  3. **此為最該優先 TDD 釘死的模組**：測試必須覆蓋「重複觸發、sensor 重啟、部分回填失敗」三種情境。
### 6.4 衍生監控指標
- **SUCCESS 但未收割的滯留時間**：若超過合理時間,代表收割管線出問題,應告警（UI 健康指標之一)。
### 6.5 故障一 vs 故障三的差別（接手者常混淆)
| | 故障一:投遞去重 | 故障三:收割冪等 |
|---|---|---|
| 位置 | 生命線**入口**（進 queue 前) | 生命線**出口**（回填 Dagster 後) |
| 防什麼 | 同一 request 被投遞兩次 → 算兩次 | 同一 request 被收割兩次 → 回填兩次 |
| 時間點 | 計算**還沒發生** | 計算**已經完成** |
| 守門員 | Task 佇列 | 收割 sensor |
兩者共用同一把冪等鍵,差別在用鑰匙的時機：故障一「投遞前比對 queue」，故障三「回填後比對 Dagster」。
---
## 7. 長期 Log 設計
**結論**：長期 log 不由 status table 寫、不由 control logic 寫,而由「產生事件的各方,在事件發生當下,各自 append 到一個獨立的 append-only event log」。
### 7.1 為什麼不是 status table
status table 的本質是「當前狀態」，是 mutable、會被覆寫的（request 從 RUNNING 變 SUCCESS,那一行被 update)。長期 log 的本質是「歷史事件」，是 immutable、append-only。兩種生命週期相反的東西不可塞進同一張表。
### 7.2 為什麼不是 control logic
Control logic 看不到所有該記的事件（worker 算完的細節、sensor 收割的時刻、data version 變化發生時它不在現場)。要它寫 log 需先把所有事件回報給它,使它成為全系統事件匯流點 → 瓶頸 + 單點故障,違反它「只決策、不搬資料」的定位。
### 7.3 推薦做法：輕量 event sourcing
```
event log 表（append-only,不可變,與 status 表分離)
  ↑ worker pool   append：task 開始 / 完成 / 失敗 / data version 算出
  ↑ sensor        append：收割發生 / materialization 回填
  ↑ control logic append：下令重跑 / 調整 worker / 取消 task
```
- **status 表 vs event log 生命週期相反**：status 表 mutable 代表「現在」（快、小、即時決策真相);event log immutable 代表「歷史」（大、append-only、衍生分析真相)。
- **誰產生事件,誰就地寫**,不經中間人 → 無瓶頸、無單點。
- **附帶好處（可審計性)**：append-only event log 天生是 task 完整時間線（投遞 → 認領 → SUBMITTED → RUNNING → 完成 → 收割),是收割縫合點 debug 的不可或缺工具。集中代寫會因回報延遲使時間線失真;各自就地寫時間戳才準確。
### 7.4 落地細節
- event log 成長快（每 task 多事件 × 10k+ task) → 需獨立 retention / 歸檔策略,寫入用 append-optimized（批次 insert、適當 index)。這也是它不該與 status 表混在一起的另一原因（存儲與 retention 需求完全不同)。
> **待接手者確認**：長期分析主要回答「運維類」（吞吐、失敗率、worker 利用率、queue 積壓趨勢)還是「資料血緣類」（某 cell 的 data version 歷史、哪些上游變更觸發哪些下游重算)。這決定 event log 記哪些事件型別與欄位。
---
## 8. Phase 1：最小可行落地（不做 message queue)
**目標**：先驗證資料身分層（Dagster lineage + 收割機制)能否運作,暫不引入 message queue 與 worker pool 的複雜度。
### 8.1 範圍
| 元件 | Phase 1 做法 |
|---|---|
| 任務佇列 | **不實作**。直接用 LSF run client 撒 job |
| 執行 | LSF run client 取代 `subprocess.run`,把 script `bsub` 到 LSF;script 結束後就地算 data version |
| 狀態儲存 | **SQLite DB**（單機,暫代 Phase 2 的 PostgreSQL) |
| 並發寫入保護 | **必須實作 write file lock**（SQLite 單寫者鎖 + 檔案鎖,避免多 worker/多 process 競爭寫入損壞) |
| Sensor | 讀 SQLite DB 看狀態;**需實作刪除機制**（收割後清理已處理記錄,避免 DB 無限膨脹) |
| 故障修復 | **暫不做**（孤兒回收、投遞去重、收割冪等的完整保證留待 Phase 2) |
| Script 正確性 | `set -e; set -o pipefail`：避免 script 中途失敗卻仍往下寫 data version 的半完成態 |
### 8.2 Phase 1 的 LSF run client 行為
```
LSF run client（取代 subprocess.run)：
  1. bsub script 到 LSF
  2. script 內：set -e; set -o pipefail → 失敗即中止,不寫 data version
  3. script 成功 → 就地算 data version
  4. 寫入 SQLite（需 write file lock 保護)：狀態 + data version
  5. sensor 輪詢 SQLite,讀狀態,回填 Dagster,並刪除已處理記錄
```
### 8.3 Phase 1 的已知限制（誠實記錄,避免接手者誤以為可上量)
- **SQLite 單寫者鎖**：高並發寫入下會成為瓶頸（單機、file lock 序列化)。Phase 1 僅適合驗證流程與中小規模,不適合 10k 級並發。
- **無故障修復**：worker 掛掉的孤兒 request 不會被回收（會永久卡住);重複投遞不去重;收割非冪等（sensor 重啟可能重複/漏回填)。**Phase 1 接受這些風險,因為目的是驗證 happy path。**
- **無 LSF 狀態同步者**：Phase 1 可能簡化為「bsub 後 worker 等結果」或「不區分 SUBMITTED/RUNNING」——但需注意這在 LSF 長排隊下會讓 worker 半阻塞。建議 Phase 1 至少在 SQLite schema 預留 SUBMITTED/RUNNING 欄位,方便 Phase 2 演進。
### 8.4 Phase 1 必須做對的事（即使簡化也不能省)
1. **Dagster 的投遞/收割分離**：即使沒有 queue,投遞 run 也不該 blocking 等計算——這是整個架構的精神,Phase 1 就要建立正確習慣。
2. **write file lock**：SQLite 並發寫入保護是正確性底線,不可省。
3. **sensor 刪除機制**：避免 SQLite 無限膨脹。
4. **冪等鍵的 schema 預留**：即使 Phase 1 不做去重/冪等回收,schema 也要先帶冪等鍵欄位,讓 Phase 2 無痛接上。
---
## 9. Phase 2：完整架構
**目標**：補上 message queue、worker pool、PostgreSQL、完整故障修復,達到可上 10k+ 規模的 production-grade。
### 9.1 範圍
| 元件 | Phase 2 做法 |
|---|---|
| 訊息佇列 | 完整 message queue,基於 **Kafka**（環境只有 Kafka,無 RabbitMQ) |
| Worker pool | 常駐 worker,pull 認領、非阻塞、動態擴縮（由 resource control 調整) |
| 狀態 DB | **PostgreSQL**（取代 SQLite,支援高並發寫入;部署於專用主機,本地磁碟存 PGDATA) |
| LSF 狀態同步者 | 實作,持續 bjobs 維護 SUBMITTED ↔ RUNNING |
| 故障修復 | 完整三種（投遞去重 / 孤兒回收 / 收割冪等),見 §6 |
| Event log | 獨立 append-only event log（見 §7) |
### 9.2 Kafka 作為 priority queue 的已知挑戰
> **環境限制**：只有 Kafka,無 RabbitMQ。Kafka 原生**不是** priority queue（它是 partition-based、FIFO-per-partition 的 log),實作跨機台的 priority 排序較困難。
接手者需面對的設計問題（本白皮書標記為待解,不預設答案)：
- **priority 可能暫時難以跨機台做到**。可能的方向（供參考,非定論)：
  - 多 topic / 多 partition 對應不同 priority tier,consumer 優先消費高 priority topic。
  - priority 排序退到「自己的調度層」：Kafka 只當傳輸,request 進 Kafka 前先由一個調度模組依 priority 決定投遞順序（呼應 §5.3「調度權留在自己的 queue」的精神)。
  - 孤兒高 priority 重排可能需要特殊 topic 或繞道處理。
- **Kafka 的 at-least-once 語意**與本架構的冪等設計是契合的（故障三本就設計成冪等 + at-least-once),這是優勢而非阻礙。
> **給接手者的方向建議**：考慮到 §5.3 已主張「投遞節流、調度權留在自己的 queue」，Kafka 在本架構中**更適合當「傳輸與緩衝」而非「優先排序器」**。priority 排序邏輯可能該放在「投遞進 Kafka 之前的自建調度層」，而非依賴 Kafka 本身的排序能力。這個方向與整體架構一致,建議優先評估。
### 9.3 從 Phase 1 遷移到 Phase 2 的關鍵點
- **SQLite → PostgreSQL**：schema 若在 Phase 1 已預留冪等鍵 + SUBMITTED/RUNNING 欄位,遷移主要是儲存層替換 + 並發能力提升。
- **直接 bsub → 經 queue**：worker pool 取代「LSF run client 直接撒」，引入 pull 認領與節流。
- **補上故障修復三件套**：建議實作順序 = 故障三（收割冪等,最影響正確性)→ 故障二（孤兒回收,否則系統默默卡死)→ 故障一（投遞去重,影響效率而非正確性,實作最直接)。
---
## 10. 與原方案的總對比表
| 面向 | 原方案（Blocking + LSF Run Launcher) | 現方案（Phase 2 目標) |
|---|---|---|
| run worker 生命週期 | = 整個計算時長 | 短命（投遞 + 收割後即釋放) |
| 同時計算數上限 | 受「同時存在幾個 run worker」限制 | 受 LSF slot 限制（Dagster 端幾乎不卡) |
| dispatch 模型 | push（daemon 逐個 launch_run) | pull（worker 自取 from queue) |
| 失敗語意 | Dagster 原生掌握（自動正確) | 自建（靠冪等鍵 + 三種故障處理保證) |
| 架構複雜度 | 低 | 高（自建 queue + worker + 狀態同步 + 故障處理) |
| 大量長計算可擴展性 | 差（daemon 啟動吞吐瓶頸) | 好 |
| lineage / 避免重複計算 | 有（Dagster 原生) | 有（Dagster 原生,保留) |
| UI 可信度 | 高（綠燈 = 計算成功) | 需自建 UI（讀執行真相,避免綠燈誤會) |
---
## 11. 給接手 agent 的關鍵提醒
1. **守住分層**：每次想「取消 Dagster 的某個追蹤功能」時,問——這是關於「執行過程」（process 活著嗎、跑到哪)還是「資料身分」（算到什麼 version、lineage)？前者該繞過,後者永遠保留。本系統的核心價值（1% 增量重跑)整個建立在後者。
2. **不要把 pull queue 當成「Dagster run queue 的替代品」**：它是活在投遞 run 之下、Dagster 看不見的執行層。Dagster 的 run queue 繼續存在,只承載少量投遞/收割 run。兩個 queue 在不同層、不同職責、共存而非替代。
3. **冪等鍵是地基**：三種故障處理共用 `(asset_key, partition, 觸發指紋)`。把這把鑰匙定義對（指紋含上游 version hash),故障處理才有共同基礎。
4. **收割 sensor 最脆弱**：它是「執行真相 → 身分真相」唯一交棒點,優先用 TDD 釘死,覆蓋重複觸發 / 重啟 / 部分失敗。
5. **SUBMITTED/RUNNING 拆分不可省**：在 LSF 數小時排隊下,這是「該加 LSF 還是加 worker」唯一診斷依據;且 SUBMITTED 孤兒判斷必須用 bjobs 而非時間。
6. **status 表 vs event log 是兩張表**：生命週期相反（mutable 當前 vs immutable 歷史),不可混為一表。
7. **Kafka 不是 priority queue**：考慮把 priority 排序放在「投遞進 Kafka 前的自建調度層」，Kafka 當傳輸與緩衝（與 §5.3 一致)。
8. **Phase 1 的簡化是刻意的**：無故障修復、用 SQLite、可能不分 SUBMITTED/RUNNING——目的是驗證 happy path。但 write file lock、投遞/收割分離、冪等鍵 schema 預留這三件即使在 Phase 1 也不能省。
---
## 12. 開放問題（待接手者與需求方確認)
1. **長期分析的主軸**：運維類 vs 資料血緣類？決定 event log 的事件型別與欄位設計（§7.4)。
2. **Kafka priority 的具體實作**：多 topic tier vs 自建調度層 vs 其他？（§9.2)
3. **Control logic 的「決定重跑哪些」**：人工在 UI 點選,還是規則自動觸發？決定 Control 與 Dagster 的 API 形狀。
4. **worker 動態擴縮的觸發條件**：看 queue 深度自動擴,還是人工調？決定 resource control 與 worker pool 的訊號介面。
5. **LSF 上同時在飛的 job 量級**：決定 bjobs 的查詢策略（批次撈 vs 分批 vs 事件機制)。
6. **單 task 最長執行時間**：決定 RUNNING 階段孤兒判斷的時間上限、以及 SUBMITTED lease 的容忍度。
