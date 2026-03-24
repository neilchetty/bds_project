#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${WORKSPACE:-$ROOT_DIR/work/server-benchmark}"
WORKFLOW="${WORKFLOW:-gene2life}"
DATA_ROOT="${DATA_ROOT:-$WORKSPACE/data}"
PROFILE="${PROFILE:-medium}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5.csv}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
export GENE2LIFE_JAVA_OPTS
COMPARE_ROUNDS="${COMPARE_ROUNDS:-4}"
MAX_NODES="${MAX_NODES:-0}"
EXECUTOR="${EXECUTOR:-hadoop}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
HDFS_DATA_ROOT="${HDFS_DATA_ROOT:-/gene2life/data/$WORKFLOW}"
HDFS_BASE_WORK_ROOT="${HDFS_BASE_WORK_ROOT:-/gene2life/work}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME:-}/etc/hadoop}"
HADOOP_FS_DEFAULT="${HADOOP_FS_DEFAULT:-}"
HADOOP_FRAMEWORK_NAME="${HADOOP_FRAMEWORK_NAME:-yarn}"
HADOOP_YARN_RM="${HADOOP_YARN_RM:-}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-1}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-3}"
REUSE_DATA="${REUSE_DATA:-true}"
GENERATE_ARGS=()
GENERATION_METADATA_FILE="$DATA_ROOT/.generation-metadata.env"

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

if [[ "$EXECUTOR" == "hadoop" ]]; then
  if [[ -z "$HADOOP_CONF_DIR" || ! -d "$HADOOP_CONF_DIR" ]]; then
    echo "HADOOP_CONF_DIR must point to a valid Hadoop configuration directory when EXECUTOR=hadoop" >&2
    exit 1
  fi
  for required in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
    if [[ ! -f "$HADOOP_CONF_DIR/$required" ]]; then
      echo "Missing $required under HADOOP_CONF_DIR=$HADOOP_CONF_DIR" >&2
      exit 1
    fi
  done
fi

if [[ ! -f "$CLUSTER_CONFIG" ]]; then
  "$ROOT_DIR/scripts/generate-cluster-config.sh" "$CLUSTER_CONFIG"
fi

mkdir -p "$WORKSPACE"
mkdir -p "$(dirname "$DATA_ROOT")"

DESIRED_METADATA=$(
  {
    printf 'workflow=%s\n' "$WORKFLOW"
    printf 'profile=%s\n' "$PROFILE"
    for ((i=0; i<${#GENERATE_ARGS[@]}; i+=2)); do
      key="${GENERATE_ARGS[i]#--}"
      value="${GENERATE_ARGS[i+1]}"
      printf '%s=%s\n' "$key" "$value"
    done
  } | LC_ALL=C sort
)

CURRENT_METADATA=""
if [[ -f "$GENERATION_METADATA_FILE" ]]; then
  CURRENT_METADATA="$(LC_ALL=C sort "$GENERATION_METADATA_FILE")"
fi

if [[ "$REUSE_DATA" == "true" && -d "$DATA_ROOT" && -n "$CURRENT_METADATA" && "$CURRENT_METADATA" == "$DESIRED_METADATA" ]]; then
  echo "Reusing existing dataset at $DATA_ROOT"
else
  rm -rf "$DATA_ROOT"
  "$ROOT_DIR/scripts/run.sh" generate-data \
    --workflow "$WORKFLOW" \
    --workspace "$WORKSPACE" \
    --data-root "$DATA_ROOT" \
    "${GENERATE_ARGS[@]}"
  printf '%s\n' "$DESIRED_METADATA" > "$GENERATION_METADATA_FILE"
fi

"$ROOT_DIR/scripts/run.sh" compare \
  --workflow "$WORKFLOW" \
  --workspace "$WORKSPACE" \
  --data-root "$DATA_ROOT" \
  --cluster-config "$CLUSTER_CONFIG" \
  --rounds "$COMPARE_ROUNDS" \
  --max-nodes "$MAX_NODES" \
  --training-warmup-runs "$TRAINING_WARMUP_RUNS" \
  --training-measure-runs "$TRAINING_MEASURE_RUNS" \
  --executor "$EXECUTOR" \
  --docker-image "$DOCKER_IMAGE" \
  --hdfs-data-root "$HDFS_DATA_ROOT" \
  --hdfs-work-root "$HDFS_BASE_WORK_ROOT" \
  --hadoop-conf-dir "$HADOOP_CONF_DIR" \
  --hadoop-fs-default "$HADOOP_FS_DEFAULT" \
  --hadoop-framework-name "$HADOOP_FRAMEWORK_NAME" \
  --hadoop-yarn-rm "$HADOOP_YARN_RM"

echo "Benchmark outputs:"
echo "  $WORKSPACE/comparison.md"
echo "  $WORKSPACE/round-01"
echo "Workflow:"
echo "  $WORKFLOW"
echo "Data root:"
echo "  $DATA_ROOT"
echo "Reuse data:"
echo "  $REUSE_DATA"
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
if [[ "$EXECUTOR" == "hadoop" ]]; then
  echo "HDFS data root:"
  echo "  $HDFS_DATA_ROOT"
  echo "HDFS work root:"
  echo "  $HDFS_BASE_WORK_ROOT"
  echo "Hadoop conf dir:"
  echo "  $HADOOP_CONF_DIR"
fi
if [[ "$MAX_NODES" != "0" ]]; then
  echo "Node limit:"
  echo "  $MAX_NODES"
fi
