#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SUBMISSION_MODE="${SUBMISSION_MODE:-true}"
FULL_SWEEP="${FULL_SWEEP:-false}"
if [[ "$FULL_SWEEP" == "true" ]]; then
  SUBMISSION_MODE="false"
fi
DEFAULT_PROFILE="small"
DEFAULT_COMPARE_ROUNDS="3"
DEFAULT_TRAINING_WARMUP_RUNS="1"
DEFAULT_TRAINING_MEASURE_RUNS="3"
if [[ "$FULL_SWEEP" == "true" ]]; then
  DEFAULT_PROFILE="medium"
  DEFAULT_COMPARE_ROUNDS="3"
  DEFAULT_TRAINING_WARMUP_RUNS="1"
  DEFAULT_TRAINING_MEASURE_RUNS="3"
fi

PROFILE="${PROFILE:-$DEFAULT_PROFILE}"
DEFAULT_WORKFLOWS="gene2life avianflu_small epigenomics"
WORKFLOWS="${WORKFLOWS:-$DEFAULT_WORKFLOWS}"
NODE_COUNTS="${NODE_COUNTS:-4 7 10 13}"
PAPER_CLUSTER_CONFIG="${PAPER_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
DENSE_CLUSTER_CONFIG="${DENSE_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-dense-28.csv}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-}"
COMPARE_ROUNDS="${COMPARE_ROUNDS:-$DEFAULT_COMPARE_ROUNDS}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-$DEFAULT_TRAINING_WARMUP_RUNS}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-$DEFAULT_TRAINING_MEASURE_RUNS}"
EXECUTOR="${EXECUTOR:-hdfs-docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-gene2life-hadoop-cluster:3.4.3}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
BASE_WORK_DIR="${BASE_WORK_DIR:-$ROOT_DIR/work/hadoop-overnight-sweeps}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/work/hadoop-overnight-logs}"
DATASET_BASE_DIR="${DATASET_BASE_DIR:-$BASE_WORK_DIR/datasets}"
REUSE_DATA="${REUSE_DATA:-true}"
SKIP_PREBUILD="${SKIP_PREBUILD:-false}"
RUN_ALL_KEEP_CLUSTER="${RUN_ALL_KEEP_CLUSTER:-false}"
HADOOP_CLUSTER_WORKDIR="${HADOOP_CLUSTER_WORKDIR:-$ROOT_DIR/work/hadoop-docker-cluster}"

mkdir -p "$BASE_WORK_DIR" "$LOG_DIR" "$DATASET_BASE_DIR"

MASTER_LOG="$LOG_DIR/master-$(date '+%Y%m%d-%H%M%S').log"
CURRENT_RUN_FILE="$LOG_DIR/current-run.txt"
COMPLETED_RUNS_FILE="$LOG_DIR/completed-runs.txt"
FAILED_RUN_FILE="$LOG_DIR/failed-run.txt"

timestamp() {
  date '+%F %T'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*" | tee -a "$MASTER_LOG"
}

cleanup_runtime() {
  rm -f "$CURRENT_RUN_FILE"
  if [[ ("$EXECUTOR" == "hadoop" || "$EXECUTOR" == "hdfs-docker") && "$RUN_ALL_KEEP_CLUSTER" != "true" ]]; then
    log "Cleaning up project Docker Hadoop cluster after sweep"
    HADOOP_CLUSTER_WORKDIR="$HADOOP_CLUSTER_WORKDIR" \
      HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
      CLUSTER_CONFIG="$PAPER_CLUSTER_CONFIG" \
      "$ROOT_DIR/scripts/cleanup-project-runtime.sh" all || true
  fi
}

trap cleanup_runtime EXIT

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
log "PAPER_CLUSTER_CONFIG=$PAPER_CLUSTER_CONFIG"
log "DENSE_CLUSTER_CONFIG=$DENSE_CLUSTER_CONFIG"
log "EXECUTOR=$EXECUTOR"
log "COMPARE_ROUNDS=$COMPARE_ROUNDS"
log "REUSE_DATA=$REUSE_DATA"
log "BASE_WORK_DIR=$BASE_WORK_DIR"
log "LOG_DIR=$LOG_DIR"
log "RUN_ALL_KEEP_CLUSTER=$RUN_ALL_KEEP_CLUSTER"
log "SUBMISSION_MODE=$SUBMISSION_MODE"

: > "$COMPLETED_RUNS_FILE"
rm -f "$FAILED_RUN_FILE" "$CURRENT_RUN_FILE"

if [[ "$SKIP_PREBUILD" != "true" ]]; then
  log "Building classes once before the sweep"
  run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build.sh"

  if [[ "$EXECUTOR" == "docker" ]]; then
    log "Building Docker image once before the sweep"
    run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
  elif [[ "$EXECUTOR" == "hadoop" || "$EXECUTOR" == "hdfs-docker" ]]; then
    log "Building Docker Hadoop cluster image once before the sweep"
    run_with_timestamped_log "$MASTER_LOG" "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$HADOOP_CLUSTER_IMAGE"
  fi
else
  log "Skipping prebuild steps because SKIP_PREBUILD=true"
fi

cluster_config_for_nodes() {
  local nodes="$1"
  if [[ -n "$CLUSTER_CONFIG" ]]; then
    printf '%s\n' "$CLUSTER_CONFIG"
    return
  fi
  if (( nodes > 13 )); then
    printf '%s\n' "$DENSE_CLUSTER_CONFIG"
  else
    printf '%s\n' "$PAPER_CLUSTER_CONFIG"
  fi
}

for workflow in $WORKFLOWS; do
  data_root="$DATASET_BASE_DIR/$workflow"
  for nodes in $NODE_COUNTS; do
    run_name="${workflow}-nodes-${nodes}"
    workspace="$BASE_WORK_DIR/$run_name"
    logfile="$LOG_DIR/$run_name.log"
    cluster_config="$(cluster_config_for_nodes "$nodes")"
    : > "$logfile"

    log "Starting $run_name"
    log "Using cluster config $cluster_config"
    printf '%s\n' "$run_name" > "$CURRENT_RUN_FILE"

    if ! run_with_timestamped_log "$logfile" env \
      WORKSPACE="$workspace" \
      WORKFLOW="$workflow" \
      DATA_ROOT="$data_root" \
      PROFILE="$PROFILE" \
      CLUSTER_CONFIG="$cluster_config" \
      MAX_NODES="$nodes" \
      COMPARE_ROUNDS="$COMPARE_ROUNDS" \
      TRAINING_WARMUP_RUNS="$TRAINING_WARMUP_RUNS" \
      TRAINING_MEASURE_RUNS="$TRAINING_MEASURE_RUNS" \
      REUSE_DATA="$REUSE_DATA" \
      HADOOP_KEEP_CLUSTER=true \
      SKIP_BUILD=true \
      EXECUTOR="$EXECUTOR" \
      DOCKER_IMAGE="$DOCKER_IMAGE" \
      HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
      GENE2LIFE_JAVA_OPTS="$GENE2LIFE_JAVA_OPTS" \
      SUBMISSION_MODE="$SUBMISSION_MODE" \
      "$ROOT_DIR/scripts/server-benchmark.sh"; then
      log "FAILED $run_name"
      printf '%s\n' "$run_name" > "$FAILED_RUN_FILE"
      exit 1
    fi

    log "Completed $run_name"
    log "Comparison report: $workspace/comparison.md"
    printf '%s\n' "$run_name" >> "$COMPLETED_RUNS_FILE"
    rm -f "$CURRENT_RUN_FILE"
  done
done

log "Sweep script completed successfully"
log "Master log: $MASTER_LOG"
