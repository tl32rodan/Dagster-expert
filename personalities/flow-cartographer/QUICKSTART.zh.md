<!-- all-might generated -->
# flow-cartographer — 快速上手(中文)

你正在跟 **flow-cartographer** 對話:給它一個執行流程(flow),它會把
這個 flow 轉成 Dagster 1.13.3,一次驗收一個增量(increment),照排程
持續跑(plan → build → verify → reflect)直到完成。它是為弱模型 + 氣隙
機台設計的,所以一切機械化、狀態都寫在磁碟上 —— 它從不依賴記住對話。

## 一次性設定(charter)

1. 指向 flow:
   ```
   setenv FLOW_SRC /abs/path/to/flow      # tcsh
   export FLOW_SRC=/abs/path/to/flow      # bash
   echo $FLOW_SRC && ls $FLOW_SRC | head  # 驗證
   ```
2. 填 charter `personalities/flow-cartographer/CONVERSION.md`:flow 名稱、
   目標、納入哪些步驟、**哪些 source 可以 PVT/cell 切分**(這些要變成
   config + generator,不是複製檔案)、限制、成功條件。把每個
   `[PLACEHOLDER]` 換掉。第一個實際目標是 **real-char pipeline**(即
   `dagster-expert/learn/09-real-flow/` 的 production 版)。

設定完就好。之後 loop 自己讀 charter;你不用手寫任務。

## 怎麼跑

Loop 由四個排程 tick 驅動(也可手動 dry-run):

| 指令 | 做什麼 |
|---|---|
| `/wake flow-cartographer plan` | 建模 flow + 產生增量 ledger |
| `/wake flow-cartographer build` | 把「一個」增量轉成 Dagster |
| `/wake flow-cartographer verify` | 自我檢查該增量 → done 或 blocked |
| `/wake flow-cartographer reflect` | 每週:從重複失敗中學習 + 重新規劃 |

每個 tick 都先讀交接(`STATUS.md` + `flow-model/_plan.yaml`),所以永遠
知道上一個 tick 做到哪 —— 這就是它「不會忘記進度」的機制。要無人值守
排程,見 `.opencode/skills/scheduling/SKILL.md`(氣隙機台用 cron)。

## 去哪看

- **現在在做什麼 / 做到哪**:`STATUS.md`(`next_action` 接力棒)與
  `flow-model/_plan.yaml`(增量 ledger)。
- **它決定不了的事**:`flow-model/_open_questions.yaml` —— 它會把問題
  park 給你,而不是亂猜。
- **它做了什麼**:`flow-model/_operations.log` + `memory/journal/<flow-name>/`。
- **某步驟為何一直失敗**:`reflect` 會在
  `memory/lessons_learned/_inbox/` 留一份提案給你看。

## 保證(guardrails)

- 只用 **public Dagster 1.13.3 API**(對照 sibling corpus 檢查);禁
  private import。
- **轉換,不亂複製**:把 source 原封複製進來 = 失敗。
- 增量沒讓 **smoke 真的跑起來** 之前,不算 done。

## Shell + 兄弟人格

tcsh 為主(`setenv` 先寫,`export` 放括號)。若要 Dagster *教學 / 維運
/ API 查詢*(不是轉換),說「switch to dagster-expert」。
