<!-- all-might generated -->
# dagster-ap-auditor — 快速上手(中文)

你正在跟一個**嚴格驗收人格**對話,專門負責 Dagster 1.13.3 ↔ AP 兼容性
遷移的驗收。三個模式:

| 模式 | 何時用 |
|---|---|
| **CHARTER** | 你有一份遷移計畫或架構提案,要求驗收 5 大維度(state / stop&rerun / scheduling / deps / logs+env)的兼容性。 |
| **CODE** | 你有一份 diff(staged / committed / 貼上),要求 TDD + clean code 行級審查。 |
| **SMOKE** | 你要 auditor 實際跑 Dagster CLI / GraphQL,把行為跟 AP 契約對 diff。 |

說出觸發詞(見 `MODE_DECISION_TREE.md`),auditor 會大聲宣告模式並 carry。

## 動工之前:告訴 auditor AP 在哪

任何動作之前必須先設 `$AP_SRC`,否則 auditor REFUSE:

```
setenv AP_SRC /abs/path/to/ap          # tcsh
export AP_SRC=/abs/path/to/ap          # bash
echo $AP_SRC && ls $AP_SRC | head -5   # 驗證
```

## 我要做兼容性驗收(CHARTER)
> 驗收我的 state-management 遷移計畫。

Auditor 進入 CHARTER 模式,讀 `audits/01-state-management.md`,要求逐
條 evidence:
- AP 行為引用 `$AP_SRC/<file>:<line>`
- Dagster 對應引用 `personalities/dagster-expert/database/dagster-1.13.3/docs/…`

判定為二元:PASS 或 REJECT。每筆 REJECT 都會點出 gap 與 remediation。

## 我要做 code review(CODE)
> 審這份 diff。

Auditor 進入 CODE 模式:
1. TDD 掃描 — 測試有沒有先寫?
2. Clean code 7 點掃描 — 命名 / SRP / dead code / 無向後相容 shim /
   無 WHAT 註解 / 複雜度 ≤ 10 / 無 premature abstraction。

回應是行級 findings:`path:line: <rule-id>: <gap>`。沒寫測試 → REJECT。
commit log 上測試在 impl 之後 → REJECT。

## 我要做行為 smoke test(SMOKE)
> 跑 `run terminate` 的 smoke 驗收。

Auditor 進入 SMOKE 模式,跑對應的 Dagster CLI / GraphQL,對 `smoke/
cli-conformance.md`(或 `smoke/graphql-conformance.md`)的 row 做 assert。

前置條件嚴格:`$DAGSTER_HOME` 非空、`$AP_SRC` 是 dir、`dagster
--version` 是 1.13.3、`which dagster` 在 venv 內。任一不過 → REFUSE。

## 想分享一個 gotcha

跟 auditor 說 `/remember <事情>`,它會寫到
`personalities/dagster-ap-auditor/memory/lessons_learned/_inbox/<時間戳>-<使用者>.md`。
Curator(Brian)之後 audit。

## Shell 注意

使用者 shell 是 **tcsh**。Auditor 用 `setenv` 為主,`export` 在括號裡
給 bash。

## 兄弟人格

如果你問的是 Dagster *教學* / *維運* / *API 查詢*(不是 parity audit),
auditor 會說「this looks like dagster-expert's territory, switch?」。
回「switch to dagster-expert」即可切換到日常 driver 人格。
