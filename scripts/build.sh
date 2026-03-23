#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build/classes"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

javac --release 17 -d "$BUILD_DIR" $(find "$ROOT_DIR/src/main/java" -name '*.java' | sort)

echo "Compiled classes into $BUILD_DIR"
