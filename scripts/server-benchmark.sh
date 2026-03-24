#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${WORKSPACE:-$ROOT_DIR/work/server-benchmark}"
WORKFLOW="${WORKFLOW:-gene2life}"
PROFILE="${PROFILE:-medium}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5.csv}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
export GENE2LIFE_JAVA_OPTS
COMPARE_ROUNDS="${COMPARE_ROUNDS:-4}"
MAX_NODES="${MAX_NODES:-0}"
EXECUTOR="${EXECUTOR:-docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-1}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-3}"
GENERATE_ARGS=()

case "$WORKFLOW" in
  gene2life)
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
    GENERATE_ARGS=(
      --query-count "$QUERY_COUNT"
      --reference-records-per-shard "$REFERENCE_RECORDS_PER_SHARD"
      --sequence-length "$SEQUENCE_LENGTH"
    )
    ;;
  avianflu_small)
    case "$PROFILE" in
      small)
        RECEPTOR_FEATURE_COUNT="${RECEPTOR_FEATURE_COUNT:-192}"
        LIGAND_ATOM_COUNT="${LIGAND_ATOM_COUNT:-32}"
        ;;
      medium)
        RECEPTOR_FEATURE_COUNT="${RECEPTOR_FEATURE_COUNT:-256}"
        LIGAND_ATOM_COUNT="${LIGAND_ATOM_COUNT:-48}"
        ;;
      large)
        RECEPTOR_FEATURE_COUNT="${RECEPTOR_FEATURE_COUNT:-320}"
        LIGAND_ATOM_COUNT="${LIGAND_ATOM_COUNT:-64}"
        ;;
      *)
        echo "Unsupported PROFILE: $PROFILE" >&2
        exit 1
        ;;
    esac
    GENERATE_ARGS=(
      --receptor-feature-count "$RECEPTOR_FEATURE_COUNT"
      --ligand-atom-count "$LIGAND_ATOM_COUNT"
    )
    ;;
  epigenomics)
    case "$PROFILE" in
      small)
        READS_PER_SPLIT="${READS_PER_SPLIT:-160}"
        READ_LENGTH="${READ_LENGTH:-72}"
        REFERENCE_RECORD_COUNT="${REFERENCE_RECORD_COUNT:-480}"
        ;;
      medium)
        READS_PER_SPLIT="${READS_PER_SPLIT:-192}"
        READ_LENGTH="${READ_LENGTH:-80}"
        REFERENCE_RECORD_COUNT="${REFERENCE_RECORD_COUNT:-640}"
        ;;
      large)
        READS_PER_SPLIT="${READS_PER_SPLIT:-224}"
        READ_LENGTH="${READ_LENGTH:-88}"
        REFERENCE_RECORD_COUNT="${REFERENCE_RECORD_COUNT:-768}"
        ;;
      *)
        echo "Unsupported PROFILE: $PROFILE" >&2
        exit 1
        ;;
    esac
    GENERATE_ARGS=(
      --reads-per-split "$READS_PER_SPLIT"
      --read-length "$READ_LENGTH"
      --reference-record-count "$REFERENCE_RECORD_COUNT"
    )
    ;;
  *)
    echo "Unsupported WORKFLOW: $WORKFLOW" >&2
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
  --workflow "$WORKFLOW" \
  --workspace "$WORKSPACE" \
  "${GENERATE_ARGS[@]}"

"$ROOT_DIR/scripts/run.sh" compare \
  --workflow "$WORKFLOW" \
  --workspace "$WORKSPACE" \
  --data-root "$WORKSPACE/data" \
  --cluster-config "$CLUSTER_CONFIG" \
  --rounds "$COMPARE_ROUNDS" \
  --max-nodes "$MAX_NODES" \
  --training-warmup-runs "$TRAINING_WARMUP_RUNS" \
  --training-measure-runs "$TRAINING_MEASURE_RUNS" \
  --executor "$EXECUTOR" \
  --docker-image "$DOCKER_IMAGE"

echo "Benchmark outputs:"
echo "  $WORKSPACE/comparison.md"
echo "  $WORKSPACE/round-01"
echo "Workflow:"
echo "  $WORKFLOW"
echo "Java options:"
echo "  $GENE2LIFE_JAVA_OPTS"
echo "Comparison rounds:"
echo "  $COMPARE_ROUNDS"
echo "WSH training runs:"
echo "  warmup=$TRAINING_WARMUP_RUNS measure=$TRAINING_MEASURE_RUNS"
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
