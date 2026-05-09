# QUICKSTART — `dagster-operator` (中文)

你是 air-gap Dagster 部署的 operator. 這份文件直接告訴你
**現在該打開哪個 skill**.

## "我從零開始"

依序閱讀:

1. **`PREREQUISITES.md`** — 確認主機、Python、Postgres、網路
   都到位.
2. **`skills/bootstrap-airgap/SKILL.md`** — 在 connected host
   建 wheelhouse, 在 air-gap host 安裝.
3. **`skills/dagster-yaml-reference/SKILL.md`** — 寫
   `$DAGSTER_HOME/dagster.yaml` (從 production 範本起步).
4. **`skills/workspace-yaml-reference/SKILL.md`** — 寫
   `workspace.yaml` (dev 用 `python_module`, 上線前切到
   `grpc_server`).
5. **`skills/start-services/SKILL.md`** — 啟動 webserver、daemon、
   code servers (附 systemd 範本).
6. **`skills/verify-deploy/SKILL.md`** — 跑 5 步驟健康檢查鏈,
   遇到第一個 fail 就停.

任何一步失敗, skill 會告訴你接下去看哪個 `diagnose-*`.

## "出狀況了"

| 症狀 | 對應 skill |
|---|---|
| Run 卡在 STARTED 無限期 | `skills/diagnose-orphan-run/` |
| "Code location failed to load" | `skills/diagnose-codeloc-fail/` |
| "Error loading base asset job" | `skills/diagnose-codeloc-fail/` (Symptom D — 跨 location AssetSpec 陷阱) |
| "ModuleNotFoundError" | `skills/diagnose-codeloc-fail/` (Symptom B) |
| Webserver 回 502 或沒回應 | `skills/verify-deploy/` |
| `dagster-daemon liveness-check` 失敗 | `skills/verify-deploy/` Step 2 + journalctl |

## "我只是要查指令"

`skills/cli-cheatsheet/` — 五個 binary, `dagster` 子指令,
以及 `dg`→`dagster` 翻譯表.

## 這個 agent **不會** 做的事

- 牽 Dagster+ / Cloud / Insights / Hybrid
- 牽 Kubernetes / Helm / `K8sRunLauncher`
- 建議 `uv`、`dg`、`pipx`、Poetry, 或 runtime 期間連公網 PyPI
- 沒有明確同意就跑 `dagster run wipe`
- 沒有確認就 `rm -rf` 使用者資料
- 開啟 telemetry

如果你叫它做以上任一項, agent 會說明為什麼超出範圍, 並提出
air-gap 友善的替代方案.

## "我想學 Dagster, 不是操作它"

切換到 `dagster-tutor` personality. 它有 8 個漸進課程
(asset → dependencies → partitions → run config → failures →
cancel/restart → cross-location → 複雜 DAG).

直接跟 agent 說: **"switch to dagster-tutor"**. Agent 會更新
`MEMORY.md` 的 `> **Active personality**:` callout 並載入 tutor
的 role. (沒有 CLI subcommand; active personality 就一行,
住在 `MEMORY.md` 裡.)

## 給 agent 回饋

發現錯誤的 recipe? Air-gap 陷阱沒被涵蓋? 用 `/remember`
丟一個 case study 進
`memory/lessons_learned/_inbox/<ISO>-<user>.md`. Brian (curator)
會稽核, 有價值的會升級成 canonical 知識.

**不要** 直接編輯 `memory/understanding/canonical.md` 或
`rules/` — 它們是 curator-only.

## 速查卡

```bash
# 啟用環境
source /opt/dagster-venv/bin/activate
export DAGSTER_HOME=/var/lib/dagster

# 健康檢查
dagster instance info
dagster-daemon liveness-check
curl -fsS http://localhost:3000/server_info
dagster definitions validate -w /etc/dagster/workspace.yaml

# 日常操作
dagster run list --limit 20
dagster definitions list -w /etc/dagster/workspace.yaml
dagster asset materialize -w /etc/dagster/workspace.yaml --select <key>

# 事後分析
dagster debug export <RUN_ID> /tmp/run.gz
dagster-webserver-debug /tmp/run.gz
```

挑一個 skill, 讀完, 執行指令, 驗證. 就這樣.
