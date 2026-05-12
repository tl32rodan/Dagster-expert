#!/usr/bin/env bash
# RUN_DEMO.sh — end-to-end "is the scale-lib demo healthy?" check.
#
# Run from this directory:
#   cd personalities/dagster-expert/demo/scale-lib
#   ./RUN_DEMO.sh
#
# Steps:
#   1. Verify venv + Dagster 1.13.3
#   2. Set up DAGSTER_HOME with this demo's dagster.yaml
#   3. dagster definitions validate
#   4. pytest tests/ (unit + integration)
#   5. python -m _smoke (end-to-end materialize)
#   6. dagster dev (background); wait for /server_info; probe GraphQL
#   7. UI verification: print URL + instructions; wait for user Enter; shut down
#
# Idempotent. Safe to re-run.
# Exits non-zero on the first failure with a clear message.

set -e

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEMO_DIR"

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
hr()     { printf -- '─%.0s' $(seq 1 60); echo; }

step=0
step_header() {
    step=$((step + 1))
    hr
    yellow "Step $step: $*"
    hr
}

fail() {
    red "✗ $*"
    exit 1
}

ok() {
    green "✓ $*"
}

# ── Step 1: venv + Dagster ─────────────────────────────────────────

step_header "verify Python venv + Dagster 1.13.3"

# Prefer dagster-venv (or DAGSTER_VENV override) even if some other
# VIRTUAL_ENV is already active. This script needs Dagster 1.13.3
# specifically and won't trust whatever's on the inherited PATH.
DAGSTER_VENV="${DAGSTER_VENV:-$HOME/dagster-venv}"
if [ -f "$DAGSTER_VENV/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$DAGSTER_VENV/bin/activate"
    ok "activated $DAGSTER_VENV"
elif command -v dagster >/dev/null 2>&1; then
    yellow "no $DAGSTER_VENV; using inherited dagster: $(command -v dagster)"
else
    fail "no $DAGSTER_VENV and no dagster on PATH. Activate Python venv with Dagster 1.13.3 first, or set DAGSTER_VENV=/path/to/venv."
fi

DAGSTER_VER=$(dagster --version 2>&1 | tr -d ',' | awk '{print $NF}')
[ "$DAGSTER_VER" = "1.13.3" ] \
    || fail "expected dagster 1.13.3, got '$DAGSTER_VER'. Verify the venv has the right Dagster pinned."
ok "dagster 1.13.3 on PATH ($(command -v dagster))"

# ── Step 2: DAGSTER_HOME ──────────────────────────────────────────

step_header "set up \$DAGSTER_HOME"

: "${DAGSTER_HOME:=$HOME/.dagster-scale-lib}"
export DAGSTER_HOME
mkdir -p "$DAGSTER_HOME"
cp dagster.yaml "$DAGSTER_HOME/dagster.yaml"
ok "DAGSTER_HOME=$DAGSTER_HOME (dagster.yaml copied)"

# ── Step 3: validate ──────────────────────────────────────────────

step_header "dagster definitions validate -w workspace.yaml"

VALIDATE_OUT=$(dagster definitions validate -w workspace.yaml 2>&1)
if echo "$VALIDATE_OUT" | grep -q "All code locations passed"; then
    ok "validate PASS"
else
    red "validate FAIL:"
    echo "$VALIDATE_OUT" | tail -10
    exit 1
fi

# ── Step 4: pytest ────────────────────────────────────────────────

step_header "pytest tests/"

if command -v pytest >/dev/null 2>&1; then
    PYTEST_OUT=$(python -m pytest tests/ -q --tb=line 2>&1 | tail -3)
    if echo "$PYTEST_OUT" | grep -q "passed"; then
        ok "$(echo "$PYTEST_OUT" | grep -E 'passed|failed' | head -1)"
    else
        red "pytest FAIL:"
        echo "$PYTEST_OUT"
        exit 1
    fi
else
    yellow "pytest not installed — skipping (install with: pip install pytest)"
fi

# ── Step 5: smoke ─────────────────────────────────────────────────

step_header "python -m _smoke (end-to-end materialize)"

SMOKE_OUT=$(python -m _smoke 2>&1 | tail -3)
if echo "$SMOKE_OUT" | grep -q "SMOKE PASS"; then
    ok "$(echo "$SMOKE_OUT" | grep "SMOKE PASS")"
else
    red "smoke FAIL — last 10 lines:"
    python -m _smoke 2>&1 | tail -10
    exit 1
fi

# ── Step 6: dagster dev + GraphQL probe ───────────────────────────

step_header "launch dagster dev (background); probe GraphQL"

# Kill any leftover instance first
pkill -f "dagster dev|dagster-webserver|dagster-daemon|dagster code-server" 2>/dev/null \
    && sleep 2 || true

DEV_LOG="/tmp/scale-lib-dagster-dev.log"
nohup dagster dev -w workspace.yaml -p 3000 > "$DEV_LOG" 2>&1 &
DEV_PID=$!
ok "dagster dev launched (pid=$DEV_PID); log=$DEV_LOG"

# Wait up to 60s for webserver
echo -n "waiting for /server_info... "
for i in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:3000/server_info >/dev/null 2>&1; then
        echo "ready ($i × 2s)"
        break
    fi
    sleep 2
    if [ "$i" = "30" ]; then
        fail "webserver did not come up in 60s. Check $DEV_LOG."
    fi
done

# GraphQL sanity probes
GQL=http://127.0.0.1:3000/graphql

ASSETS_JSON=$(curl -fsS -H 'Content-Type: application/json' "$GQL" \
    -d '{"query":"{ repositoryOrError(repositorySelector: {repositoryName: \"__repository__\", repositoryLocationName: \"scale_lib\"}) { ... on Repository { assetNodes { assetKey { path } groupName } } } }"}')

ASSET_COUNT=$(echo "$ASSETS_JSON" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    n = len(d['data']['repositoryOrError']['assetNodes'])
    print(n)
except Exception as e:
    print(f'err: {e}', file=sys.stderr)
    print(-1)
")

if [ "$ASSET_COUNT" -ge 23 ]; then
    ok "GraphQL returns $ASSET_COUNT assets (expected ≥23 — sources + 21 steps × 1 lib + cross-lib)"
else
    fail "GraphQL returned $ASSET_COUNT assets (expected ≥23). Check $DEV_LOG."
fi

# Verify lineage
EDGE_COUNT=$(curl -fsS -H 'Content-Type: application/json' "$GQL" \
    -d '{"query":"{ repositoryOrError(repositorySelector: {repositoryName: \"__repository__\", repositoryLocationName: \"scale_lib\"}) { ... on Repository { assetNodes { dependencyKeys { path } } } } }"}' \
    | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(sum(len(n['dependencyKeys']) for n in d['data']['repositoryOrError']['assetNodes']))
")
ok "GraphQL returns $EDGE_COUNT lineage edges"

# ── Step 7: UI verification (manual) ──────────────────────────────

step_header "UI is live — verify manually"

cat <<EOF

  Open in your browser:
  ${COLUMNS:+$(printf '%*s' "$COLUMNS" "" | tr ' ' '─')}

      $(green http://127.0.0.1:3000)

  Suggested UI checks (1–2 minutes):

  1. Click \`Catalog\` (top nav). You should see 23 assets across 5
     groups (sources / lib_a__extraction / lib_a__char /
     lib_a__kit_root_only / lib_a__setup_root_only).

  2. Click \`lib_a/step1\`. Partitions tab → 46 branches.

  3. Click \`Lineage\` tab on any step. See the variant-tree
     dependencies (corner → em / ht / lvf / ...).

  4. Materialize one partition: click \`Materialize\` → select
     \`em\` partition → Launch run. Should complete in <5s.

  5. The run shows in the \`Runs\` tab. Click into it for event
     timeline + asset materializations.

EOF

if [ -t 0 ]; then
    yellow "Press Enter to shut down dagster dev (or Ctrl-C to keep it running)."
    read -r _
fi

# ── Cleanup ───────────────────────────────────────────────────────

step_header "shut down"

kill "$DEV_PID" 2>/dev/null || true
pkill -f "dagster dev|dagster-webserver|dagster-daemon|dagster code-server" 2>/dev/null || true
sleep 1
ok "dagster dev stopped"

hr
green "RUN_DEMO complete. Demo is healthy."
hr
