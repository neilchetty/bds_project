#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_TS="${RUN_TS:-$(date '+%Y%m%d-%H%M%S')}"
SESSION_DIR="${RUNNER_SESSION_DIR:-$ROOT_DIR/work/overnight-run-$RUN_TS}"
SESSION_DIR="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$SESSION_DIR")"
CONSOLE_LOG="$SESSION_DIR/runner-console.log"
PID_FILE="$SESSION_DIR/runner.pid"
SUBMISSION_MODE="${SUBMISSION_MODE:-true}"

mkdir -p "$SESSION_DIR"

nohup env RUNNER_SESSION_DIR="$SESSION_DIR" SUBMISSION_MODE="$SUBMISSION_MODE" bash "$ROOT_DIR/commands.sh" "$SESSION_DIR" >"$CONSOLE_LOG" 2>&1 < /dev/null &
PID="$!"
echo "$PID" > "$PID_FILE"

echo "Started submission run in background"
echo "PID: $PID"
echo "Session directory: $SESSION_DIR"
echo "Console log: $CONSOLE_LOG"
echo "PID file: $PID_FILE"
echo "Tail log with:"
echo "  tail -f '$CONSOLE_LOG'"
echo "Live status with:"
echo "  $ROOT_DIR/scripts/submission-status.sh '$SESSION_DIR'"
echo "When complete, reports will be written to:"
echo "  $SESSION_DIR/report.html"
echo "  $SESSION_DIR/report.pdf"
echo "  $SESSION_DIR/transcript.log"
