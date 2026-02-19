#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/build/native"

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

if [[ "${OPENFGDB4J_TARGET_OS}" == "windows" ]]; then
  echo "build-native.sh is for Unix targets. Use openfgdb4j/scripts/ci/build-and-test-windows.ps1" >&2
  exit 1
fi

if [[ -n "${CMAKE_BIN:-}" ]]; then
  CMAKE_BIN="${CMAKE_BIN}"
elif [[ -x "/Applications/CMake.app/Contents/bin/cmake" ]]; then
  CMAKE_BIN="/Applications/CMake.app/Contents/bin/cmake"
else
  CMAKE_BIN="cmake"
fi
CMAKE_GENERATOR="${OPENFGDB4J_CMAKE_GENERATOR:-Unix Makefiles}"
GDAL_MINIMAL_STAGE="${OPENFGDB4J_GDAL_MINIMAL_ROOT:-$ROOT_DIR/gdal-minimal/build/stage}"

export OPENFGDB4J_GDAL_MINIMAL_ROOT="$GDAL_MINIMAL_STAGE"
export CMAKE_BIN
export OPENFGDB4J_CMAKE_GENERATOR="$CMAKE_GENERATOR"

"$ROOT_DIR/gdal-minimal/scripts/build-all.sh"

mkdir -p "$BUILD_DIR"

"$CMAKE_BIN" -S "$PROJECT_DIR/native" -B "$BUILD_DIR" -G "$CMAKE_GENERATOR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DOPENFGDB4J_GDAL_MINIMAL_ROOT="$GDAL_MINIMAL_STAGE"
"$CMAKE_BIN" --build "$BUILD_DIR" --config Release
