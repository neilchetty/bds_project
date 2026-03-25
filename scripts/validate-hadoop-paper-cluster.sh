#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/work/hadoop-paper-cluster}"
MASTER_SERVICE="${MASTER_SERVICE:-master}"

if [[ ! -f "$OUTPUT_DIR/docker-compose.yml" ]]; then
  echo "Missing $OUTPUT_DIR/docker-compose.yml" >&2
  exit 1
fi

expected_nodes="$(wc -l < "$OUTPUT_DIR/selected-nodes.csv" | tr -d ' ')"

echo "Compose services:"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" ps

echo
echo "Worker DNS from master:"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc '
  for host in $(printf "%s" "$WORKER_HOSTS" | tr "," " "); do
    getent hosts "$host"
  done
'

echo
echo "HDFS live datanodes:"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc 'hdfs dfsadmin -report'

live_datanodes="$(docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes ([[:space:]]*\\([0-9][0-9]*\\)).*/\\1/p' | tail -1" | tr -d '\r[:space:]')"
if [[ -z "$live_datanodes" ]]; then
  live_datanodes="$(docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes ([[:space:]]*\\([0-9][0-9]*\\)).*/\\1/p' | tail -1" | tr -dc '0-9')"
fi

echo
echo "YARN nodes:"
docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc 'yarn node -list'

registered_nodes="$(docker compose -f "$OUTPUT_DIR/docker-compose.yml" exec -T "$MASTER_SERVICE" bash -lc "yarn node -list 2>/dev/null | sed -n 's/^Total Nodes:[[:space:]]*//p' | tail -1" | tr -d '\r[:space:]')"

if [[ "${live_datanodes:-0}" != "$expected_nodes" ]]; then
  echo "Expected $expected_nodes live DataNodes but found ${live_datanodes:-0}" >&2
  exit 1
fi

if [[ "${registered_nodes:-0}" != "$expected_nodes" ]]; then
  echo "Expected $expected_nodes YARN NodeManagers but found ${registered_nodes:-0}" >&2
  exit 1
fi

echo
echo "Cluster validation passed for $expected_nodes workers."
