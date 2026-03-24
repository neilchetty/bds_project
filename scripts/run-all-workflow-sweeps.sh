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
EXECUTOR="${EXECUTOR:-hadoop}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
BASE_WORK_DIR="${BASE_WORK_DIR:-$ROOT_DIR/work/paper-sweeps}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/work/paper-sweep-logs}"
DATASET_BASE_DIR="${DATASET_BASE_DIR:-$BASE_WORK_DIR/datasets}"
REUSE_DATA="${REUSE_DATA:-true}"
HDFS_DATASET_BASE_DIR="${HDFS_DATASET_BASE_DIR:-/gene2life/data}"
HDFS_BASE_WORK_ROOT="${HDFS_BASE_WORK_ROOT:-/gene2life/work}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME:-}/etc/hadoop}"
HADOOP_FS_DEFAULT="${HADOOP_FS_DEFAULT:-}"
HADOOP_FRAMEWORK_NAME="${HADOOP_FRAMEWORK_NAME:-yarn}"
HADOOP_YARN_RM="${HADOOP_YARN_RM:-}"
HADOOP_ENABLE_NODE_LABELS="${HADOOP_ENABLE_NODE_LABELS:-false}"

mkdir -p "$BASE_WORK_DIR" "$LOG_DIR" "$DATASET_BASE_DIR"

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
  if [[ "$logfile" == "$MASTER_LOG" ]]; then
    "$@" 2>&1 | while IFS= read -r line; do
      printf '[%s] %s\n' "$(timestamp)" "$line"
    done | tee -a "$MASTER_LOG"
  else
    "$@" 2>&1 | while IFS= read -r line; do
      printf '[%s] %s\n' "$(timestamp)" "$line"
    done | tee -a "$logfile" | tee -a "$MASTER_LOG"
  fi
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
log "REUSE_DATA=$REUSE_DATA"
if [[ "$EXECUTOR" == "hadoop" ]]; then
  log "HDFS_DATASET_BASE_DIR=$HDFS_DATASET_BASE_DIR"
  log "HDFS_BASE_WORK_ROOT=$HDFS_BASE_WORK_ROOT"
  log "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
  log "HADOOP_ENABLE_NODE_LABELS=$HADOOP_ENABLE_NODE_LABELS"
fi

log "Building classes once before the sweep"
run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build.sh"

if [[ "$EXECUTOR" == "docker" ]]; then
  log "Building Docker image once before the sweep"
  run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
fi

for workflow in $WORKFLOWS; do
  data_root="$DATASET_BASE_DIR/$workflow"
  for nodes in $NODE_COUNTS; do
    run_name="${workflow}-nodes-${nodes}"
    workspace="$BASE_WORK_DIR/$run_name"
    logfile="$LOG_DIR/$run_name.log"
    : > "$logfile"

    log "Starting $run_name"

    if ! run_with_timestamped_log "$logfile" env \
      WORKSPACE="$workspace" \
      WORKFLOW="$workflow" \
      DATA_ROOT="$data_root" \
      PROFILE="$PROFILE" \
      CLUSTER_CONFIG="$CLUSTER_CONFIG" \
      MAX_NODES="$nodes" \
      COMPARE_ROUNDS="$COMPARE_ROUNDS" \
      TRAINING_WARMUP_RUNS="$TRAINING_WARMUP_RUNS" \
      TRAINING_MEASURE_RUNS="$TRAINING_MEASURE_RUNS" \
      REUSE_DATA="$REUSE_DATA" \
      EXECUTOR="$EXECUTOR" \
      DOCKER_IMAGE="$DOCKER_IMAGE" \
      HDFS_DATA_ROOT="$HDFS_DATASET_BASE_DIR/$workflow" \
      HDFS_BASE_WORK_ROOT="$HDFS_BASE_WORK_ROOT" \
      HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
      HADOOP_FS_DEFAULT="$HADOOP_FS_DEFAULT" \
      HADOOP_FRAMEWORK_NAME="$HADOOP_FRAMEWORK_NAME" \
      HADOOP_YARN_RM="$HADOOP_YARN_RM" \
      HADOOP_ENABLE_NODE_LABELS="$HADOOP_ENABLE_NODE_LABELS" \
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
