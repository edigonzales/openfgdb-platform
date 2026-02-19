#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GDAL_MINIMAL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST_DIR="$GDAL_MINIMAL_DIR/manifests"
VERSIONS_FILE="$MANIFEST_DIR/versions.lock"
SHA_FILE="$MANIFEST_DIR/SHA256SUMS"

DOWNLOAD_DIR="$GDAL_MINIMAL_DIR/third_party/downloads"
SRC_DIR="$GDAL_MINIMAL_DIR/third_party/src"
BUILD_WORK_DIR="$GDAL_MINIMAL_DIR/build/work"
STAGE_DIR="${OPENFGDB4J_GDAL_MINIMAL_ROOT:-$GDAL_MINIMAL_DIR/build/stage}"

if [[ -n "${CMAKE_BIN:-}" ]]; then
  CMAKE_BIN="${CMAKE_BIN}"
elif [[ -x "/Applications/CMake.app/Contents/bin/cmake" ]]; then
  CMAKE_BIN="/Applications/CMake.app/Contents/bin/cmake"
else
  CMAKE_BIN="cmake"
fi
CMAKE_GENERATOR="${OPENFGDB4J_CMAKE_GENERATOR:-Unix Makefiles}"

normalize_target_os() {
  local raw="${1:-}"
  raw="$(echo "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    darwin|mac|macos|osx) echo "macos" ;;
    linux) echo "linux" ;;
    windows|win|mingw*|msys*|cygwin*) echo "windows" ;;
    *) echo "$raw" ;;
  esac
}

normalize_target_arch() {
  local raw="${1:-}"
  raw="$(echo "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) echo "$raw" ;;
  esac
}

TARGET_OS="${OPENFGDB4J_TARGET_OS:-$(uname -s)}"
TARGET_ARCH="${OPENFGDB4J_TARGET_ARCH:-$(uname -m)}"
TARGET_OS="$(normalize_target_os "$TARGET_OS")"
TARGET_ARCH="$(normalize_target_arch "$TARGET_ARCH")"

cpu_count() {
  local n=""
  if command -v nproc >/dev/null 2>&1; then
    n="$(nproc)"
  elif command -v sysctl >/dev/null 2>&1; then
    n="$(sysctl -n hw.logicalcpu 2>/dev/null || true)"
  fi
  if [[ -z "$n" || "$n" -le 0 ]]; then
    n=4
  fi
  echo "$n"
}

JOBS="${OPENFGDB4J_GDAL_MINIMAL_JOBS:-$(cpu_count)}"

load_versions() {
  if [[ ! -f "$VERSIONS_FILE" ]]; then
    echo "Missing versions file: $VERSIONS_FILE" >&2
    exit 1
  fi
  # shellcheck source=/dev/null
  source "$VERSIONS_FILE"
}

require_cmd() {
  local cmd
  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "Required command not found: $cmd" >&2
      exit 1
    fi
  done
}

ensure_dirs() {
  mkdir -p "$DOWNLOAD_DIR" "$SRC_DIR" "$BUILD_WORK_DIR" "$STAGE_DIR"
}

sha256_file() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file_path" | awk '{print $1}'
    return
  fi
  echo "No SHA256 tool found (need sha256sum or shasum)" >&2
  exit 1
}

expected_sha() {
  local archive_name="$1"
  awk -v n="$archive_name" '$2 == n { print $1 }' "$SHA_FILE"
}

verify_sha() {
  local file_path="$1"
  local archive_name="$2"
  local expected
  expected="$(expected_sha "$archive_name")"
  if [[ -z "$expected" ]]; then
    echo "No SHA256 pinned for $archive_name in $SHA_FILE" >&2
    exit 1
  fi
  local actual
  actual="$(sha256_file "$file_path")"
  if [[ "$actual" != "$expected" ]]; then
    echo "SHA256 mismatch for $archive_name" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    exit 1
  fi
}

download_archive() {
  local url="$1"
  local archive_name="$2"
  local out="$DOWNLOAD_DIR/$archive_name"

  if [[ -f "$out" ]]; then
    verify_sha "$out" "$archive_name"
    echo "Using cached archive: $archive_name"
    return
  fi

  local tmp="$out.part"
  echo "Downloading $archive_name"
  curl -fL "$url" -o "$tmp"
  verify_sha "$tmp" "$archive_name"
  mv "$tmp" "$out"
}

extract_archive_to() {
  local archive_name="$1"
  local target_dir_name="$2"
  local archive="$DOWNLOAD_DIR/$archive_name"
  local dest="$SRC_DIR/$target_dir_name"
  local tmp_root="$BUILD_WORK_DIR/extract-$target_dir_name"

  if [[ ! -f "$archive" ]]; then
    echo "Archive not found: $archive" >&2
    exit 1
  fi

  rm -rf "$tmp_root" "$dest"
  mkdir -p "$tmp_root"
  tar -xzf "$archive" -C "$tmp_root"

  local top
  top="$(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  if [[ -z "$top" ]]; then
    echo "Failed to find extracted top-level directory for $archive_name" >&2
    exit 1
  fi

  mv "$top" "$dest"
  rm -rf "$tmp_root"
}

stage_lib() {
  local name="$1"
  echo "$STAGE_DIR/lib/$name"
}

stage_include_dir() {
  echo "$STAGE_DIR/include"
}

macos_arch_name() {
  case "$TARGET_ARCH" in
    amd64) echo "x86_64" ;;
    arm64) echo "arm64" ;;
    *)
      echo "Unsupported macOS arch: $TARGET_ARCH" >&2
      exit 1
      ;;
  esac
}

append_macos_cmake_arch() {
  local array_name="$1"
  if [[ "$TARGET_OS" == "macos" ]]; then
    local mac_arch
    mac_arch="$(macos_arch_name)"
    eval "$array_name+=(\"-DCMAKE_OSX_ARCHITECTURES=$mac_arch\")"
  fi
}

append_macos_cflags() {
  local array_name="$1"
  if [[ "$TARGET_OS" == "macos" ]]; then
    local mac_arch
    mac_arch="$(macos_arch_name)"
    eval "$array_name+=(\"-arch\" \"$mac_arch\")"
  fi
}
