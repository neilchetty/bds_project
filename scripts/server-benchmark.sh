#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${WORKSPACE:-$ROOT_DIR/work/server-benchmark}"
PROFILE="${PROFILE:-medium}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5.csv}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
export GENE2LIFE_JAVA_OPTS
COMPARE_ROUNDS="${COMPARE_ROUNDS:-4}"
MAX_NODES="${MAX_NODES:-0}"
EXECUTOR="${EXECUTOR:-docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"

case "$PROFILE" in
  small)
    QUERY_COUNT="${QUERY_COUNT:-192}"
    REFERENCE_RECORDS_PER_SHARD="${REFERENCE_RECORDS_PER_SHARD:-180000}"
    SEQUENCE_LENGTH="${SEQUENCE_LENGTH:-280}"
    ;;
  medium)
    QUERY_COUNT="${QUERY_COUNT:-256}"
    REFERENCE_RECORDS_PER_SHARD="${REFERENCE_RECORDS_PER_SHARD:-300000}"
    SEQUENCE_LENGTH="${SEQUENCE_LENGTH:-320}"
    ;;
  large)
    QUERY_COUNT="${QUERY_COUNT:-320}"
    REFERENCE_RECORDS_PER_SHARD="${REFERENCE_RECORDS_PER_SHARD:-500000}"
    SEQUENCE_LENGTH="${SEQUENCE_LENGTH:-360}"
    ;;
  *)
    echo "Unsupported PROFILE: $PROFILE" >&2
    exit 1
    ;;
esac

"$ROOT_DIR/scripts/build.sh"

if [[ "$EXECUTOR" == "docker" ]]; then
  "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
fi

if [[ ! -f "$CLUSTER_CONFIG" ]]; then
  "$ROOT_DIR/scripts/generate-cluster-config.sh" "$CLUSTER_CONFIG"
fi

"$ROOT_DIR/scripts/run.sh" generate-data \
  --workspace "$WORKSPACE" \
  --query-count "$QUERY_COUNT" \
  --reference-records-per-shard "$REFERENCE_RECORDS_PER_SHARD" \
  --sequence-length "$SEQUENCE_LENGTH"

"$ROOT_DIR/scripts/run.sh" compare \
  --workspace "$WORKSPACE" \
  --data-root "$WORKSPACE/data" \
  --cluster-config "$CLUSTER_CONFIG" \
  --rounds "$COMPARE_ROUNDS" \
  --max-nodes "$MAX_NODES" \
  --executor "$EXECUTOR" \
  --docker-image "$DOCKER_IMAGE"

echo "Benchmark outputs:"
echo "  $WORKSPACE/comparison.md"
echo "  $WORKSPACE/round-01"
echo "Java options:"
echo "  $GENE2LIFE_JAVA_OPTS"
echo "Comparison rounds:"
echo "  $COMPARE_ROUNDS"
echo "Executor:"
echo "  $EXECUTOR"
if [[ "$EXECUTOR" == "docker" ]]; then
  echo "Docker image:"
  echo "  $DOCKER_IMAGE"
fi
if [[ "$MAX_NODES" != "0" ]]; then
  echo "Node limit:"
  echo "  $MAX_NODES"
fi
