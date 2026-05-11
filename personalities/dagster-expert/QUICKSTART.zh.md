<!-- all-might generated -->
# dagster-expert — 快速上手(中文)

這是一個有三個模式的單一 personality:**TEACHER**(教學)、
**OPERATOR**(維運)、**LIBRARIAN**(API 查詢)。你只要告訴 agent 你要
做什麼,它會根據 `ROLE.md::§0` 的觸發表自動選模式。

## 我想學 Dagster
> 帶我做 lesson 01。

Agent 進入 **TEACHER 模式**,逐步帶你過 `learn/01-asset-and-materialize/`。
第一個指令之前 agent 會要你設**每個 lesson 自己的** `DAGSTER_HOME`
(例如 `setenv DAGSTER_HOME ~/.dagster-tutor/01-asset-and-materialize`),
確保不同 lesson 的 runs / storage 不會互相污染。11 個 lesson,從 01
(最小 asset)到 11(multi-library + UI scaling);每次換 lesson agent
都會提醒你重新設定 DAGSTER_HOME。

## 我要在 air-gap 機器上裝 / 跑 Dagster
> 在這台 air-gap 機器上裝 Dagster。

Agent 進入 **OPERATOR 模式**,讀 `skills/bootstrap-airgap/SKILL.md`,
帶你走 wheelhouse 流程(在連網機器 `pip download` → 拷貝 → 在 air-gap
機器 `pip install --no-index --find-links=…`)。Agent **拒絕** `uv` /
`dg` / k8s / 公開 PyPI。

其他 operator 任務(診斷卡住的 run、設定 dagster.yaml、重啟 daemon)
直接描述問題,觸發表會路由。

## 我要查 API
> 1.13.3 的 partitions API 怎麼用?

Agent 進入 **LIBRARIAN 模式**,讀
`database/dagster-1.13.3/docs/partitions.md`,給你公開 API 簽名 + 一個
可跑的 `database/dagster-1.13.3/examples/` 範例。如果沒有對應條目,
agent **拒絕** 從訓練記憶生成,並請你補一個 case study。

## 中途切換模式
說「switch to operator」(或 `…to teacher` / `…to librarian`)即可。
Agent 會確認,下一個 turn 開始切換。

## 想分享一個 gotcha
跟 agent 說 `/remember <事情>`,它會寫到
`memory/lessons_learned/_inbox/<時間戳>-<使用者>.md`。Curator(Brian)
之後會 audit。

## Shell 注意
使用者環境是 **tcsh**。Agent 用 `setenv` 語法為主;`export` 用括號
標註(for bash)。
