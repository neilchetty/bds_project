#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_JAR="$ROOT_DIR/target/gene2life-app.jar"

if [[ ! -f "$APP_JAR" ]]; then
  "$ROOT_DIR/scripts/build.sh"
fi

JAVA_OPTS_STRING="${GENE2LIFE_JAVA_OPTS:-${JAVA_OPTS:-}}"

if [[ -n "$JAVA_OPTS_STRING" ]]; then
  read -r -a JAVA_OPTS_ARRAY <<< "$JAVA_OPTS_STRING"
  java "${JAVA_OPTS_ARRAY[@]}" -jar "$APP_JAR" "$@"
else
  java -jar "$APP_JAR" "$@"
fi
