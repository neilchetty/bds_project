#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROFILE="${PROFILE:-medium}"
WORKFLOWS="${WORKFLOWS:-gene2life avianflu_small epigenomics}"
NODE_COUNTS="${NODE_COUNTS:-4 7 10 12}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
COMPARE_ROUNDS="${COMPARE_ROUNDS:-4}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-1}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-3}"
EXECUTOR="${EXECUTOR:-docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
BASE_WORK_DIR="${BASE_WORK_DIR:-$ROOT_DIR/work/paper-sweeps}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/work/paper-sweep-logs}"

mkdir -p "$BASE_WORK_DIR" "$LOG_DIR"

MASTER_LOG="$LOG_DIR/master-$(date '+%Y%m%d-%H%M%S').log"

timestamp() {
  date '+%F %T'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*" | tee -a "$MASTER_LOG"
}

run_with_timestamped_log() {
  local logfile="$1"
  shift
  set +e
  "$@" 2>&1 | while IFS= read -r line; do
    printf '[%s] %s\n' "$(timestamp)" "$line"
  done | tee -a "$logfile" | tee -a "$MASTER_LOG"
  local statuses=("${PIPESTATUS[@]}")
  set -e
  return "${statuses[0]}"
}

log "Sweep script started"
log "PROFILE=$PROFILE"
log "WORKFLOWS=$WORKFLOWS"
log "NODE_COUNTS=$NODE_COUNTS"
log "CLUSTER_CONFIG=$CLUSTER_CONFIG"
log "EXECUTOR=$EXECUTOR"
log "COMPARE_ROUNDS=$COMPARE_ROUNDS"

log "Building classes once before the sweep"
run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build.sh"

if [[ "$EXECUTOR" == "docker" ]]; then
  log "Building Docker image once before the sweep"
  run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
fi

for workflow in $WORKFLOWS; do
  for nodes in $NODE_COUNTS; do
    run_name="${workflow}-nodes-${nodes}"
    workspace="$BASE_WORK_DIR/$run_name"
    logfile="$LOG_DIR/$run_name.log"
    : > "$logfile"

    log "Starting $run_name"

    if ! run_with_timestamped_log "$logfile" env \
      WORKSPACE="$workspace" \
      WORKFLOW="$workflow" \
      PROFILE="$PROFILE" \
      CLUSTER_CONFIG="$CLUSTER_CONFIG" \
      MAX_NODES="$nodes" \
      COMPARE_ROUNDS="$COMPARE_ROUNDS" \
      TRAINING_WARMUP_RUNS="$TRAINING_WARMUP_RUNS" \
      TRAINING_MEASURE_RUNS="$TRAINING_MEASURE_RUNS" \
      EXECUTOR="$EXECUTOR" \
      DOCKER_IMAGE="$DOCKER_IMAGE" \
      GENE2LIFE_JAVA_OPTS="$GENE2LIFE_JAVA_OPTS" \
      "$ROOT_DIR/scripts/server-benchmark.sh"; then
      log "FAILED $run_name"
      exit 1
    fi

    log "Completed $run_name"
    log "Comparison report: $workspace/comparison.md"
  done
done

log "Sweep script completed successfully"
log "Master log: $MASTER_LOG"
