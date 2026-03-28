#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$ROOT_DIR/target/gene2life-app.jar" ]]; then
  "$ROOT_DIR/scripts/build.sh"
fi

JAVA_OPTS_STRING="${GENE2LIFE_JAVA_OPTS:-${JAVA_OPTS:-}}"

if [[ -n "$JAVA_OPTS_STRING" ]]; then
  read -r -a JAVA_OPTS_ARRAY <<< "$JAVA_OPTS_STRING"
  java "${JAVA_OPTS_ARRAY[@]}" -jar "$ROOT_DIR/target/gene2life-app.jar" "$@"
else
  java -jar "$ROOT_DIR/target/gene2life-app.jar" "$@"
fi
