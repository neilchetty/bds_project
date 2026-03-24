#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROFILE="${PROFILE:-medium}"
WORKFLOWS="${WORKFLOWS:-gene2life avianflu_small epigenomics}"
TOTAL_NODE_COUNTS="${TOTAL_NODE_COUNTS:-4 7 10 13}"
WORKER_NODE_COUNTS="${WORKER_NODE_COUNTS:-}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
COMPARE_ROUNDS="${COMPARE_ROUNDS:-4}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-1}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-3}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
BASE_WORK_DIR="${BASE_WORK_DIR:-$ROOT_DIR/work/paper-hadoop-sweeps}"
BASE_CLUSTER_DIR="${BASE_CLUSTER_DIR:-$ROOT_DIR/work/paper-hadoop-clusters}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/work/paper-hadoop-logs}"
DATASET_BASE_DIR="${DATASET_BASE_DIR:-$BASE_WORK_DIR/datasets}"
HDFS_DATASET_BASE_DIR="${HDFS_DATASET_BASE_DIR:-/gene2life/data}"
HDFS_BASE_WORK_ROOT="${HDFS_BASE_WORK_ROOT:-/gene2life/work}"
IMAGE_TAG="${IMAGE_TAG:-gene2life-hadoop-cluster:3.4.3}"
REUSE_DATA="${REUSE_DATA:-true}"

mkdir -p "$BASE_WORK_DIR" "$BASE_CLUSTER_DIR" "$LOG_DIR" "$DATASET_BASE_DIR"

MASTER_LOG="$LOG_DIR/master-$(date '+%Y%m%d-%H%M%S').log"

timestamp() {
  date '+%F %T'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*" | tee -a "$MASTER_LOG"
}

run_with_log() {
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

stop_cluster() {
  local cluster_output_dir="$1"
  if [[ -f "$cluster_output_dir/docker-compose.yml" ]]; then
    OUTPUT_DIR="$cluster_output_dir" "$ROOT_DIR/scripts/stop-hadoop-paper-cluster.sh" >/dev/null 2>&1 || true
  fi
}

log "Paper-close Hadoop sweep started"
log "PROFILE=$PROFILE"
log "WORKFLOWS=$WORKFLOWS"
log "CLUSTER_CONFIG=$CLUSTER_CONFIG"
log "COMPARE_ROUNDS=$COMPARE_ROUNDS"
log "REUSE_DATA=$REUSE_DATA"
log "IMAGE_TAG=$IMAGE_TAG"

if [[ -n "$WORKER_NODE_COUNTS" ]]; then
  log "Using explicit worker-node counts: $WORKER_NODE_COUNTS"
else
  log "Using paper total-node counts (master included): $TOTAL_NODE_COUNTS"
fi

run_with_log "$MASTER_LOG" "$ROOT_DIR/scripts/build.sh"
run_with_log "$MASTER_LOG" "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$IMAGE_TAG"

if [[ -n "$WORKER_NODE_COUNTS" ]]; then
  sweep_targets="$WORKER_NODE_COUNTS"
else
  sweep_targets="$TOTAL_NODE_COUNTS"
fi

for target in $sweep_targets; do
  if [[ -n "$WORKER_NODE_COUNTS" ]]; then
    worker_nodes="$target"
    total_nodes="$((target + 1))"
    cluster_label="workers-${worker_nodes}"
  else
    total_nodes="$target"
    worker_nodes="$((target - 1))"
    if (( worker_nodes <= 0 )); then
      echo "Invalid TOTAL_NODE_COUNTS entry: $target" >&2
      exit 1
    fi
    cluster_label="total-${total_nodes}"
  fi

  cluster_output_dir="$BASE_CLUSTER_DIR/$cluster_label"
  cluster_log="$LOG_DIR/cluster-$cluster_label.log"
  : > "$cluster_log"

  log "Starting multi-worker Hadoop cluster for $cluster_label (workers=$worker_nodes total=$total_nodes)"
  if ! run_with_log "$cluster_log" env \
    OUTPUT_DIR="$cluster_output_dir" \
    MAX_NODES="$worker_nodes" \
    CLUSTER_CONFIG="$CLUSTER_CONFIG" \
    IMAGE_TAG="$IMAGE_TAG" \
    SKIP_IMAGE_BUILD="true" \
    "$ROOT_DIR/scripts/start-hadoop-paper-cluster.sh"; then
    log "FAILED starting cluster $cluster_label"
    exit 1
  fi

  trap 'stop_cluster "$cluster_output_dir"' EXIT
  source "$cluster_output_dir/cluster.env"

  for workflow in $WORKFLOWS; do
    workspace="$BASE_WORK_DIR/${workflow}-${cluster_label}"
    data_root="$DATASET_BASE_DIR/$workflow"
    run_log="$LOG_DIR/${workflow}-${cluster_label}.log"
    : > "$run_log"

    log "Running $workflow on $cluster_label"
    if ! run_with_log "$run_log" env \
      WORKSPACE="$workspace" \
      WORKFLOW="$workflow" \
      DATA_ROOT="$data_root" \
      PROFILE="$PROFILE" \
      CLUSTER_CONFIG="$CLUSTER_CONFIG" \
      MAX_NODES="$worker_nodes" \
      COMPARE_ROUNDS="$COMPARE_ROUNDS" \
      TRAINING_WARMUP_RUNS="$TRAINING_WARMUP_RUNS" \
      TRAINING_MEASURE_RUNS="$TRAINING_MEASURE_RUNS" \
      REUSE_DATA="$REUSE_DATA" \
      EXECUTOR="hadoop" \
      HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
      HADOOP_FS_DEFAULT="$HADOOP_FS_DEFAULT" \
      HADOOP_FRAMEWORK_NAME="${HADOOP_FRAMEWORK_NAME:-yarn}" \
      HADOOP_YARN_RM="$HADOOP_YARN_RM" \
      HADOOP_ENABLE_NODE_LABELS="${HADOOP_ENABLE_NODE_LABELS:-true}" \
      HDFS_DATA_ROOT="$HDFS_DATASET_BASE_DIR/$workflow" \
      HDFS_BASE_WORK_ROOT="$HDFS_BASE_WORK_ROOT" \
      GENE2LIFE_JAVA_OPTS="$GENE2LIFE_JAVA_OPTS" \
      "$ROOT_DIR/scripts/server-benchmark.sh"; then
      log "FAILED $workflow on $cluster_label"
      exit 1
    fi

    log "Completed $workflow on $cluster_label"
    log "Comparison report: $workspace/comparison.md"
  done

  stop_cluster "$cluster_output_dir"
  trap - EXIT
  log "Stopped cluster $cluster_label"
done

log "Paper-close Hadoop sweep completed successfully"
log "Master log: $MASTER_LOG"
