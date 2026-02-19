#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

if [[ -z "${OPENFGDB4J_TARGET_OS:-}" ]]; then
  uname_s="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$uname_s" in
    darwin*) export OPENFGDB4J_TARGET_OS=macos ;;
    linux*) export OPENFGDB4J_TARGET_OS=linux ;;
    *) export OPENFGDB4J_TARGET_OS="$uname_s" ;;
  esac
fi
if [[ -z "${OPENFGDB4J_TARGET_ARCH:-}" ]]; then
  uname_m="$(uname -m | tr '[:upper:]' '[:lower:]')"
  case "$uname_m" in
    x86_64|amd64) export OPENFGDB4J_TARGET_ARCH=amd64 ;;
    arm64|aarch64) export OPENFGDB4J_TARGET_ARCH=arm64 ;;
    *) export OPENFGDB4J_TARGET_ARCH="$uname_m" ;;
  esac
fi

export OPENFGDB4J_CMAKE_GENERATOR="${OPENFGDB4J_CMAKE_GENERATOR:-Unix Makefiles}"

JAVAC_BIN="${JAVAC_BIN:-javac}"
JAVA_BIN="${JAVA_BIN:-java}"
JAR_BIN="${JAR_BIN:-jar}"

export JAVAC_BIN
export JAR_BIN

"$PROJECT_DIR/scripts/build-native.sh"
if [[ "${OPENFGDB4J_RUN_JEXTRACT:-0}" == "1" ]]; then
  "$PROJECT_DIR/scripts/generate-bindings.sh"
fi
"$PROJECT_DIR/scripts/build-java.sh"

LIB_EXT=".so"
if [[ "${OPENFGDB4J_TARGET_OS}" == "macos" ]]; then
  LIB_EXT=".dylib"
fi

LIB_PATH="$PROJECT_DIR/build/native/libopenfgdb$LIB_EXT"
if [[ ! -f "$LIB_PATH" ]]; then
  alt="$(find "$PROJECT_DIR/build/native" -maxdepth 3 -type f -name "libopenfgdb*" | head -n 1 || true)"
  if [[ -z "$alt" ]]; then
    echo "Native library not found in $PROJECT_DIR/build/native" >&2
    exit 1
  fi
  LIB_PATH="$alt"
fi

CI_CLASS_DIR="$PROJECT_DIR/build/ci-classes"
rm -rf "$CI_CLASS_DIR"
mkdir -p "$CI_CLASS_DIR"

CI_SOURCES=()
while IFS= read -r source_file; do
  CI_SOURCES+=("$source_file")
done < <(find "$PROJECT_DIR/src/test/java/ch/ehi/openfgdb4j/ci" -name '*.java' -print)
if [[ ${#CI_SOURCES[@]} -eq 0 ]]; then
  echo "No CI smoke sources found under src/test/java/ch/ehi/openfgdb4j/ci" >&2
  exit 1
fi

OPENFGDB_JAR="$PROJECT_DIR/build/java/openfgdb4j.jar"
"$JAVAC_BIN" --release 22 -cp "$OPENFGDB_JAR" -d "$CI_CLASS_DIR" "${CI_SOURCES[@]}"

run_scenario() {
  local backend="$1"
  local scenario="$2"
  shift 2
  OPENFGDB4J_BACKEND="$backend" "$@" "$JAVA_BIN" --enable-native-access=ALL-UNNAMED \
    -Dopenfgdb4j.lib="$LIB_PATH" \
    -cp "$CI_CLASS_DIR:$OPENFGDB_JAR" \
    ch.ehi.openfgdb4j.ci.OpenFgdbCiSmokeMain "$scenario"
}

run_scenario gdal gdal env
run_scenario adapter adapter env
run_scenario gdal gdal-fail env OPENFGDB4J_GDAL_FORCE_FAIL=1
run_scenario invalid invalid-backend env

"$PROJECT_DIR/scripts/ci/check-linkage-unix.sh" "$LIB_PATH"

echo "build-and-test-unix.sh OK for ${OPENFGDB4J_TARGET_OS}/${OPENFGDB4J_TARGET_ARCH}"
