#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

if [[ -z "${OPENFGDB4J_TARGET_OS:-}" ]]; then
  uname_s="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$uname_s" in
    darwin*) OPENFGDB4J_TARGET_OS=macos ;;
    linux*) OPENFGDB4J_TARGET_OS=linux ;;
    *) OPENFGDB4J_TARGET_OS="$uname_s" ;;
  esac
fi
if [[ -z "${OPENFGDB4J_TARGET_ARCH:-}" ]]; then
  uname_m="$(uname -m | tr '[:upper:]' '[:lower:]')"
  case "$uname_m" in
    x86_64|amd64) OPENFGDB4J_TARGET_ARCH=amd64 ;;
    aarch64|arm64) OPENFGDB4J_TARGET_ARCH=arm64 ;;
    *) OPENFGDB4J_TARGET_ARCH="$uname_m" ;;
  esac
fi

ARTIFACT_NAME="openfgdb4j-bin-${OPENFGDB4J_TARGET_OS}-${OPENFGDB4J_TARGET_ARCH}"
if [[ $# -ge 1 && -n "$1" ]]; then
  ARTIFACT_DIR="$1"
else
  ARTIFACT_DIR="$PROJECT_DIR/build/artifacts/$ARTIFACT_NAME"
fi

mkdir -p "$ARTIFACT_DIR/native" "$ARTIFACT_DIR/java" "$ARTIFACT_DIR/include" "$ARTIFACT_DIR/metadata"

OPENFGDB_JAR="$PROJECT_DIR/build/java/openfgdb4j.jar"
if [[ ! -f "$OPENFGDB_JAR" ]]; then
  echo "Missing jar: $OPENFGDB_JAR" >&2
  exit 1
fi

LIB_CANDIDATE=""
if [[ "$OPENFGDB4J_TARGET_OS" == "macos" ]]; then
  LIB_CANDIDATE="$PROJECT_DIR/build/native/libopenfgdb.dylib"
elif [[ "$OPENFGDB4J_TARGET_OS" == "linux" ]]; then
  LIB_CANDIDATE="$PROJECT_DIR/build/native/libopenfgdb.so"
else
  LIB_CANDIDATE="$PROJECT_DIR/build/native/openfgdb.dll"
fi
if [[ ! -f "$LIB_CANDIDATE" ]]; then
  LIB_CANDIDATE="$(find "$PROJECT_DIR/build/native" -maxdepth 3 -type f \( -name 'libopenfgdb.*' -o -name 'openfgdb.dll' \) | head -n 1 || true)"
fi
if [[ -z "$LIB_CANDIDATE" || ! -f "$LIB_CANDIDATE" ]]; then
  echo "Missing native library under $PROJECT_DIR/build/native" >&2
  exit 1
fi

cp "$LIB_CANDIDATE" "$ARTIFACT_DIR/native/"
cp "$OPENFGDB_JAR" "$ARTIFACT_DIR/java/"
cp "$PROJECT_DIR/native/include/openfgdb_c_api.h" "$ARTIFACT_DIR/include/"

GIT_SHA="unknown"
if command -v git >/dev/null 2>&1; then
  GIT_SHA="$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
fi

cat > "$ARTIFACT_DIR/metadata/manifest.json" <<JSON
{
  "name": "$ARTIFACT_NAME",
  "target_os": "$OPENFGDB4J_TARGET_OS",
  "target_arch": "$OPENFGDB4J_TARGET_ARCH",
  "git_sha": "$GIT_SHA",
  "native_library": "native/$(basename "$LIB_CANDIDATE")",
  "java_jar": "java/openfgdb4j.jar",
  "c_api_header": "include/openfgdb_c_api.h"
}
JSON

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
    return
  fi
  echo "No sha256 tool found" >&2
  exit 1
}

{
  for file in \
    "$ARTIFACT_DIR/native/$(basename "$LIB_CANDIDATE")" \
    "$ARTIFACT_DIR/java/openfgdb4j.jar" \
    "$ARTIFACT_DIR/include/openfgdb_c_api.h" \
    "$ARTIFACT_DIR/metadata/manifest.json"; do
    printf "%s  %s\n" "$(sha256 "$file")" "${file#$ARTIFACT_DIR/}"
  done
} > "$ARTIFACT_DIR/metadata/sha256sums.txt"

echo "Packaged artifact: $ARTIFACT_DIR"
