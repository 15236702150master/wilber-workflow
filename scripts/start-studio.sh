#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/.wilberflow-studio"
PID_FILE="$PID_DIR/serve.pid"
LOG_FILE="$PID_DIR/serve.log"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8765}"

mkdir -p "$PID_DIR"

if [ ! -x "$ROOT_DIR/.venv/bin/python" ]; then
  echo "error: virtualenv not found. Run 'bash scripts/setup.sh' first." >&2
  exit 1
fi

if [ -f "$PID_FILE" ]; then
  OLD_PID="$(cat "$PID_FILE")"
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" >/dev/null 2>&1; then
    echo "studio already running at http://$HOST:$PORT (pid=$OLD_PID)"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

cd "$ROOT_DIR"
nohup env PYTHONPATH=src "$ROOT_DIR/.venv/bin/python" -m wilberflow.cli serve --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" >"$PID_FILE"

sleep 1
if ! kill -0 "$NEW_PID" >/dev/null 2>&1; then
  echo "error: studio failed to start. Check $LOG_FILE" >&2
  exit 1
fi

cat <<EOF
Studio started.
URL: http://$HOST:$PORT
PID: $NEW_PID
Log: $LOG_FILE
Stop: bash scripts/stop-studio.sh
EOF
