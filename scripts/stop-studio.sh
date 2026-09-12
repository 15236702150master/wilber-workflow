#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/.wilberflow-studio/serve.pid"
BRIDGE_STOPPER="$ROOT_DIR/scripts/stop-bridge.sh"

if [ ! -f "$PID_FILE" ]; then
  echo "studio is not running (no pid file)."
else
  PID="$(cat "$PID_FILE")"
  if [ -n "$PID" ] && kill -0 "$PID" >/dev/null 2>&1; then
    kill "$PID"
    echo "studio stopped (pid=$PID)"
  else
    echo "studio already stopped."
  fi
fi

rm -f "$PID_FILE"

if [ -f "$BRIDGE_STOPPER" ]; then
  bash "$BRIDGE_STOPPER"
fi
