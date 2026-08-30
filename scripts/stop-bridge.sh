#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRAPPER_PS1="$ROOT_DIR/scripts/run_windows_ps_admin.ps1"
TEARDOWN_PS1="$ROOT_DIR/scripts/teardown_windows_chrome_cdp_bridge.ps1"
BRIDGE_URL="${WILBERFLOW_BRIDGE_URL:-http://127.0.0.1:9223/json/version}"
BRIDGE_TIMEOUT_SECONDS="${WILBERFLOW_BRIDGE_TIMEOUT_SECONDS:-20}"

bridge_ready() {
  powershell.exe -NoProfile -ExecutionPolicy Bypass -Command \
    "try { \$r = Invoke-WebRequest -UseBasicParsing -Uri '$BRIDGE_URL' -TimeoutSec 2; if (\$r.StatusCode -eq 200) { 'ready' } } catch { '' }" \
    2>/dev/null | tr -d '\r' | grep -q '^ready$'
}

if [ "${WILBERFLOW_SKIP_BRIDGE:-0}" = "1" ]; then
  echo "bridge stop skipped (WILBERFLOW_SKIP_BRIDGE=1)."
  exit 0
fi

if ! command -v powershell.exe >/dev/null 2>&1; then
  echo "warning: powershell.exe not found; skip bridge stop." >&2
  exit 0
fi

WIN_WRAPPER="$(wslpath -w "$WRAPPER_PS1")"
WIN_TEARDOWN="$(wslpath -w "$TEARDOWN_PS1")"

if [ "${WILBERFLOW_BRIDGE_DRY_RUN:-0}" = "1" ]; then
  echo "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"$WIN_WRAPPER\" -ScriptPath \"$WIN_TEARDOWN\" -WaitForExit"
  exit 0
fi

echo "stopping Windows Chrome CDP bridge (may prompt for UAC)..."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$WIN_WRAPPER" -ScriptPath "$WIN_TEARDOWN" -WaitForExit >/dev/null

for ((i = 0; i < BRIDGE_TIMEOUT_SECONDS; i++)); do
  if ! bridge_ready; then
    echo "bridge stopped"
    exit 0
  fi
  sleep 1
done

echo "warning: bridge stop was triggered but shutdown was not confirmed within ${BRIDGE_TIMEOUT_SECONDS}s." >&2
exit 0
