#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION_DIR="${1:-$(ls -1dt "$ROOT_DIR"/work/overnight-run-* 2>/dev/null | head -n 1)}"
if [[ -z "$SESSION_DIR" || ! -d "$SESSION_DIR" ]]; then
  echo "No submission session found" >&2
  exit 1
fi
SESSION_DIR="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$SESSION_DIR")"
LOG_DIR="$SESSION_DIR/sweep-logs"
RUN_ENV="$SESSION_DIR/run.env"
WORKFLOWS="gene2life avianflu_small"
NODE_COUNTS="2 4 12 28"
CURRENT_RUN_FILE="$LOG_DIR/current-run.txt"
COMPLETED_RUNS_FILE="$LOG_DIR/completed-runs.txt"
FAILED_RUN_FILE="$LOG_DIR/failed-run.txt"
COMPOSE_FILE="$SESSION_DIR/hadoop-docker-cluster/docker-compose.yml"

read_run_env_value() {
  local key="$1"
  local default_value="$2"
  if [[ ! -f "$RUN_ENV" ]]; then
    printf '%s\n' "$default_value"
    return
  fi
  python3 - "$RUN_ENV" "$key" "$default_value" <<'PY'
import pathlib
import shlex
import sys

path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
default = sys.argv[3]
raw_value = None
for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
    if line.startswith(key + "="):
        raw_value = line.split("=", 1)[1]
        break
if raw_value is None:
    print(default)
    raise SystemExit(0)
try:
    parsed = shlex.split(raw_value)
except ValueError:
    parsed = []
if raw_value.startswith(("'", '"')) or "\\" in raw_value or len(parsed) == 1:
    print(parsed[0] if parsed else raw_value)
else:
    print(raw_value)
PY
}

RUN_STATUS="$(read_run_env_value RUN_STATUS UNKNOWN)"
WORKFLOWS="$(read_run_env_value WORKFLOWS "$WORKFLOWS")"
NODE_COUNTS="$(read_run_env_value NODE_COUNTS "$NODE_COUNTS")"

derived_status="$RUN_STATUS"
pending_count=0
completed_count=0
failed_count=0
current_run=""
if [[ -f "$CURRENT_RUN_FILE" ]]; then
  current_run="$(cat "$CURRENT_RUN_FILE")"
fi
for workflow in $WORKFLOWS; do
  for nodes in $NODE_COUNTS; do
    run_name="${workflow}-nodes-${nodes}"
    comparison="$SESSION_DIR/workspaces/$run_name/comparison.md"
    if [[ -f "$comparison" ]]; then
      completed_count=$((completed_count + 1))
    elif [[ -f "$FAILED_RUN_FILE" && "$(cat "$FAILED_RUN_FILE")" == "$run_name" ]]; then
      failed_count=$((failed_count + 1))
    else
      pending_count=$((pending_count + 1))
    fi
  done
done
if [[ "$derived_status" == "SUCCESS" && "$pending_count" -gt 0 ]]; then
  derived_status="PARTIAL"
fi

echo "Session: $SESSION_DIR"
echo "Recorded status: $derived_status"
if [[ -n "$current_run" ]]; then
  echo "Current run: $current_run"
fi
if [[ -f "$FAILED_RUN_FILE" ]]; then
  echo "Failed run: $(cat "$FAILED_RUN_FILE")"
fi
echo
echo "Matrix:"
for workflow in $WORKFLOWS; do
  for nodes in $NODE_COUNTS; do
    run_name="${workflow}-nodes-${nodes}"
    comparison="$SESSION_DIR/workspaces/$run_name/comparison.md"
    if [[ -f "$comparison" ]]; then
      echo "  $run_name: COMPLETED"
    elif [[ -f "$FAILED_RUN_FILE" && "$(cat "$FAILED_RUN_FILE")" == "$run_name" ]]; then
      echo "  $run_name: FAILED"
    elif [[ -n "$current_run" && "$current_run" == "$run_name" ]]; then
      echo "  $run_name: RUNNING"
    else
      echo "  $run_name: PENDING"
    fi
  done
done

echo
echo "Counts: completed=$completed_count failed=$failed_count pending=$pending_count"

echo
echo "Repo-owned processes:"
ps -ef | grep -E "commands.sh $SESSION_DIR|run-all-workflow-sweeps.sh|server-benchmark.sh|gene2life-app.jar" | grep -v grep || true

if [[ -f "$COMPOSE_FILE" ]]; then
  echo
  echo "Docker containers:"
  docker ps --format 'table {{.Names}}\t{{.Status}}' | grep 'hadoop-paper-cluster' || true
  echo
  echo "Docker CPU summary:"
  docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' | grep 'hadoop-paper-cluster' || true
  echo
  echo "YARN applications:"
  docker compose -p hadoop-paper-cluster -f "$COMPOSE_FILE" exec -T master yarn application -list -appStates RUNNING,ACCEPTED,NEW 2>/dev/null || true
fi

echo
echo "Recent log tail:"
tail -n 40 "$SESSION_DIR/runner-console.log" 2>/dev/null || tail -n 40 "$SESSION_DIR/transcript.log"
