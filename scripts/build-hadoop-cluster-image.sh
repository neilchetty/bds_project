#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="${1:-gene2life-hadoop-cluster:3.4.3}"

docker build -t "$IMAGE_TAG" -f "$ROOT_DIR/docker/hadoop-cluster/Dockerfile" "$ROOT_DIR"

echo "Built Hadoop cluster image $IMAGE_TAG"
