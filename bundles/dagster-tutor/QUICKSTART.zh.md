# QUICKSTART — `dagster-tutor` (中文)

你想學 Dagster. 這個 personality 有 8 個漸進課程, 挑一個起點.

## "我完全沒碰過 Dagster"

從 01 開始按順序做. 每課 30–90 分鐘.

直接跟 agent 說 (沒有 CLI subcommand, switch 是用講的):
- "switch to dagster-tutor" — agent 會更新 `MEMORY.md` 的 `> **Active personality**:`
- 然後 "讓我們開始 01-asset-and-materialize"

Agent 會帶你跑: 讀 README → 看程式 → 跑起來 → 做練習 →
回顧. 全 8 課大約 6–8 小時.

## "我已經會 X, 從哪繼續?"

| 你已會 | 從這裡開始 |
|---|---|
| `@asset` + Materialize 按鈕 | 02-deps-and-lineage |
| Asset deps 與 lineage | 03-partitions |
| Static partitions | 04-runconfig (或想專注 ops 就跳 06) |
| Run config 與 failures 基礎 | 06-interrupt-rerun (recovery 語意) |
| Multi-code-location workspace | 07 (Day-7 federation bug 案例) |
| 上述全部 | 08-complex-deps (sparse-matrix DAG) |

## "我有具體問題"

不用挑 lesson, 直接問. Agent 會幫你 route:

- "怎麼把 run 參數化?" → lesson 04
- "Cancel 一個 run 會發生什麼?" → lesson 06a
- "跨 library 的 dependency 為什麼怪怪的?" → lesson 07

如果問題對不上任何 lesson, agent 會用 general knowledge 回答
並標 confidence, 必要時 `/remember` 一個 case study.

## "我自己做, 不要 walkthrough"

每課都自包:

```bash
cd personalities/dagster-tutor/learn/<NN-topic>/
# 讀 README.md
dagster dev -m <module-name>     # 大部分課程, 看 README
# 或
dagster dev -w workspace.yaml    # lesson 07, 08
```

打開 http://127.0.0.1:3000.

## 這個 personality **不會** 做

- 在 air-gap host 上裝 Dagster (那是
  `dagster-operator` 的 `bootstrap-airgap`)
- Production 故障診斷 (`dagster-operator` 的 `diagnose-*`)
- 建議 `uv`、`dg`、`pipx`、k8s、Dagster+ — 超出範圍

如果你問, agent 會說 "switch 到 `dagster-operator`" — 那就跟它
說 "switch to dagster-operator", 它會幫你更新 `MEMORY.md` 的
active personality callout (一行字). 沒有 CLI subcommand.

## 課程目錄 (一行摘要)

| # | 主題 | 學完帶走什麼 |
|---|---|---|
| 01 | Asset & materialize | 最小 Dagster 迴圈 |
| 02 | Dependencies & lineage | Dagster 跟 job runner 的真正差異 |
| 03 | Partitions | Dagster 版的 "for-loop" |
| 04 | Run config | 從 UI 把 run 參數化 |
| 05 | Failures, retries | Asset 層級的失敗語意 |
| 06 | Interrupt + rerun | Run state machine 的 recovery |
| 07 | Cross-location | 多 team / 多 library DAG (含 Day-7 bug) |
| 08 | Complex deps | Sparse-matrix DAG (route A vs B) |

## 版本與前置

- Dagster 1.13.3 (全部課程鎖死)
- Python 3.10+
- 一個 venv, 有 `dagster==1.13.3` 與 `dagster-webserver==1.13.3`
- Air-gap 安裝: 切到 `dagster-operator`, 跑 `bootstrap-airgap`

## 想給回饋?

如果某課把你卡住了, 或範例錯了, 跟 agent 說. 它會丟一個
case study 到 `memory/lessons_learned/_inbox/`. Brian (curator)
審查, 該更新就更新, 否則歸檔到 `_reviewed/`.

不要直接改 lesson README — 是 curator-only.

## 開始?

挑一課:
- 新手: "讓我們做 01"
- 接續: "讓我們做 06"
- 直接問題: 直接打字問

Agent 會跟 `skills/walkthrough-lesson/SKILL.md` 的流程走 —
README → 程式 → 跑 → 觀察 → 練習 → 回顧.
