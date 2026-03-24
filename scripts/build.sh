#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
rm -rf "$ROOT_DIR/target"

if command -v mvn >/dev/null 2>&1; then
  mvn -q -DskipTests package
elif command -v docker >/dev/null 2>&1; then
  mkdir -p "$ROOT_DIR/.m2"
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "$ROOT_DIR":/workspace \
    -v "$ROOT_DIR/.m2":/tmp/m2 \
    -w /workspace \
    -e MAVEN_CONFIG=/tmp/m2 \
    -e HOME=/tmp \
    maven:3.9.9-eclipse-temurin-17 \
    mvn -q -DskipTests -Dmaven.repo.local=/tmp/m2/repository package
else
  echo "Neither mvn nor docker is available for building the project." >&2
  exit 1
fi

echo "Built $ROOT_DIR/target/gene2life-app.jar"
