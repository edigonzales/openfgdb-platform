#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
BUILD_CLASSES_DIR="$PROJECT_DIR/build/classes"
BUILD_JAR_DIR="$PROJECT_DIR/build/java"
JAVAC_BIN="${JAVAC_BIN:-javac}"
JAR_BIN="${JAR_BIN:-jar}"

rm -rf "$BUILD_CLASSES_DIR"
mkdir -p "$BUILD_CLASSES_DIR" "$BUILD_JAR_DIR"

JAVA_SOURCES=()
while IFS= read -r source_file; do
  JAVA_SOURCES+=("$source_file")
done < <(find "$PROJECT_DIR/src/main/java" "$PROJECT_DIR/src/generated/java" -name '*.java' -print)

if [[ ${#JAVA_SOURCES[@]} -eq 0 ]]; then
  echo "No Java sources found"
  exit 1
fi

"$JAVAC_BIN" --release 22 -d "$BUILD_CLASSES_DIR" "${JAVA_SOURCES[@]}"
"$JAR_BIN" --create --file "$BUILD_JAR_DIR/openfgdb4j.jar" -C "$BUILD_CLASSES_DIR" .
