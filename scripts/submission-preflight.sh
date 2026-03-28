#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFLIGHT_DIR="${1:-${PREFLIGHT_DIR:-$ROOT_DIR/work/submission-preflight}}"
PREFLIGHT_DIR="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$PREFLIGHT_DIR")"
PREFLIGHT_LOG="${PREFLIGHT_DIR}/preflight.log"
SMOKE_WORKSPACE="${PREFLIGHT_DIR}/benchmark-smoke"
SMOKE_CLUSTER_DIR="${PREFLIGHT_DIR}/hadoop-docker-cluster"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-gene2life-hadoop-cluster:3.4.3}"
PAPER_CLUSTER_CONFIG="${PAPER_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
DENSE_CLUSTER_CONFIG="${DENSE_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-dense-28.csv}"

mkdir -p "$PREFLIGHT_DIR"
: > "$PREFLIGHT_LOG"

timestamp() {
  date '+%F %T'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*" | tee -a "$PREFLIGHT_LOG"
}

run_cmd() {
  log "COMMAND: $*"
  "$@" 2>&1 | tee -a "$PREFLIGHT_LOG"
}

cleanup() {
  HADOOP_CLUSTER_WORKDIR="$SMOKE_CLUSTER_DIR" \
    HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
    CLUSTER_CONFIG="$PAPER_CLUSTER_CONFIG" \
    "$ROOT_DIR/scripts/cleanup-project-runtime.sh" all >/dev/null 2>&1 || true
}

trap cleanup EXIT

log "Submission preflight directory: $PREFLIGHT_DIR"
run_cmd "$ROOT_DIR/scripts/build.sh"
run_cmd "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
run_cmd "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$HADOOP_CLUSTER_IMAGE"
run_cmd "$ROOT_DIR/scripts/validate-docker-node-pinning.sh" "$PAPER_CLUSTER_CONFIG" "$DOCKER_IMAGE"
run_cmd "$ROOT_DIR/scripts/validate-docker-node-pinning.sh" "$DENSE_CLUSTER_CONFIG" "$DOCKER_IMAGE"
run_cmd "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" up "$PAPER_CLUSTER_CONFIG" "$SMOKE_CLUSTER_DIR" "$HADOOP_CLUSTER_IMAGE"
run_cmd "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" health "$PAPER_CLUSTER_CONFIG" "$SMOKE_CLUSTER_DIR" "$HADOOP_CLUSTER_IMAGE"
run_cmd "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" validate "$PAPER_CLUSTER_CONFIG" "$SMOKE_CLUSTER_DIR" "$HADOOP_CLUSTER_IMAGE"
run_cmd env \
  EXECUTOR=hadoop \
  WORKFLOW=gene2life \
  PROFILE=small \
  COMPARE_ROUNDS=1 \
  TRAINING_WARMUP_RUNS=0 \
  TRAINING_MEASURE_RUNS=1 \
  MAX_NODES=2 \
  SKIP_BUILD=true \
  REUSE_DATA=false \
  HADOOP_KEEP_CLUSTER=true \
  WORKSPACE="$SMOKE_WORKSPACE" \
  DATA_ROOT="$SMOKE_WORKSPACE/data" \
  HADOOP_CLUSTER_WORKDIR="$SMOKE_CLUSTER_DIR" \
  "$ROOT_DIR/scripts/server-benchmark.sh"

if [[ ! -f "$SMOKE_WORKSPACE/comparison.md" ]]; then
  log "Preflight smoke did not produce $SMOKE_WORKSPACE/comparison.md"
  exit 1
fi

log "Submission preflight succeeded"
log "Smoke comparison report: $SMOKE_WORKSPACE/comparison.md"
