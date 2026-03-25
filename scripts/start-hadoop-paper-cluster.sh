#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/work/hadoop-paper-cluster}"
MAX_NODES="${MAX_NODES:-0}"
IMAGE_TAG="${IMAGE_TAG:-gene2life-hadoop-cluster:3.4.3}"
MASTER_SERVICE="${MASTER_SERVICE:-master}"
SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-false}"

if [[ "$SKIP_IMAGE_BUILD" != "true" ]]; then
  "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$IMAGE_TAG"
fi
MAX_NODES="$MAX_NODES" IMAGE_TAG="$IMAGE_TAG" MASTER_SERVICE="$MASTER_SERVICE" \
  "$ROOT_DIR/scripts/generate-hadoop-paper-cluster.sh" "$CLUSTER_CONFIG" "$OUTPUT_DIR"

docker compose -f "$OUTPUT_DIR/docker-compose.yml" up -d

ready_hdfs=false
for _ in $(seq 1 60); do
  if docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -ls / >/dev/null 2>&1"; then
    ready_hdfs=true
    break
  fi
  sleep 2
done
if [[ "$ready_hdfs" != "true" ]]; then
  echo "HDFS did not become ready for cluster under $OUTPUT_DIR" >&2
  exit 1
fi

expected_nodes="$(wc -l < "$OUTPUT_DIR/selected-nodes.csv" | tr -d ' ')"
client_user="$(id -un)"
ready_datanodes=false
for _ in $(seq 1 90); do
  live_datanodes="$(docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes ([[:space:]]*\\([0-9][0-9]*\\)).*/\\1/p' | tail -1" | tr -dc '0-9')"
  if [[ "${live_datanodes:-0}" == "$expected_nodes" ]]; then
    ready_datanodes=true
    break
  fi
  sleep 2
done
if [[ "$ready_datanodes" != "true" ]]; then
  echo "HDFS did not register the expected $expected_nodes live DataNodes for cluster under $OUTPUT_DIR" >&2
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfsadmin -report || true" >&2 || true
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" ps >&2 || true
  exit 1
fi

ready_yarn=false
for _ in $(seq 1 90); do
  listed="$(docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "yarn node -list 2>/dev/null | sed -n 's/^Total Nodes:[[:space:]]*//p' | tail -1" | tr -d '\r')"
  if [[ "${listed:-0}" == "$expected_nodes" ]]; then
    ready_yarn=true
    break
  fi
  sleep 2
done
if [[ "$ready_yarn" != "true" ]]; then
  echo "YARN did not register the expected $expected_nodes workers for cluster under $OUTPUT_DIR" >&2
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "yarn node -list || true" >&2 || true
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" ps >&2 || true
  exit 1
fi

ready_safemode=false
for _ in $(seq 1 60); do
  safemode_state="$(docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfsadmin -safemode get 2>/dev/null | tail -1" | tr -d '\r')"
  if [[ "$safemode_state" == *"OFF"* ]]; then
    ready_safemode=true
    break
  fi
  sleep 2
done
if [[ "$ready_safemode" != "true" ]]; then
  echo "HDFS did not leave safe mode for cluster under $OUTPUT_DIR" >&2
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfsadmin -safemode get || true" >&2 || true
  exit 1
fi

docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -mkdir -p /gene2life /gene2life/data /gene2life/work /user/${client_user} /user/${client_user}/.staging >/dev/null 2>&1 || true"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -chmod 1777 /gene2life /gene2life/data /gene2life/work >/dev/null 2>&1 || true"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -chmod 755 /user >/dev/null 2>&1 || true"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -chown ${client_user} /user/${client_user} /user/${client_user}/.staging >/dev/null 2>&1 || true"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -chmod 700 /user/${client_user} >/dev/null 2>&1 || true"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfs -chmod 700 /user/${client_user}/.staging >/dev/null 2>&1 || true"

labels="$(tr -d '\n' < "$OUTPUT_DIR/node-labels.txt")"
mapping="$(cat "$OUTPUT_DIR/node-mapping.txt")"
if [[ -n "$labels" ]]; then
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "yarn rmadmin -addToClusterNodeLabels '$labels' >/dev/null 2>&1 || true"
  docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "yarn rmadmin -replaceLabelsOnNode '$mapping'"
fi

echo "Hadoop paper cluster is up."
echo "Source this before running the project from the host:"
echo "  source $OUTPUT_DIR/cluster.env"
echo "Worker count:"
echo "  $expected_nodes"
echo "Total container count (master + workers):"
echo "  $((expected_nodes + 1))"
echo "Validation command:"
echo "  OUTPUT_DIR=$OUTPUT_DIR $ROOT_DIR/scripts/validate-hadoop-paper-cluster.sh"
echo "Node mappings:"
echo "  $mapping"
