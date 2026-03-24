#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/work/hadoop-paper-cluster}"

docker compose -f "$OUTPUT_DIR/docker-compose.yml" down -v

echo "Stopped Hadoop paper cluster defined in $OUTPUT_DIR/docker-compose.yml"
