#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -d "$ROOT_DIR/build/classes" ]]; then
  "$ROOT_DIR/scripts/build.sh"
fi

JAVA_OPTS_STRING="${GENE2LIFE_JAVA_OPTS:-${JAVA_OPTS:-}}"

if [[ -n "$JAVA_OPTS_STRING" ]]; then
  read -r -a JAVA_OPTS_ARRAY <<< "$JAVA_OPTS_STRING"
  java "${JAVA_OPTS_ARRAY[@]}" -cp "$ROOT_DIR/build/classes" org.gene2life.cli.Main "$@"
else
  java -cp "$ROOT_DIR/build/classes" org.gene2life.cli.Main "$@"
fi
