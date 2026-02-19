#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"

require_cmd make
load_versions
ensure_dirs

if [[ "${OPENFGDB4J_GDAL_MINIMAL_REBUILD:-0}" != "1" && -f "$(stage_lib libsqlite3.a)" && -x "$STAGE_DIR/bin/sqlite3" ]]; then
  echo "sqlite already built: $(stage_lib libsqlite3.a)"
  exit 0
fi

if [[ ! -d "$SRC_DIR/$SQLITE_SRC_DIR" || ! -d "$SRC_DIR/$PROJ_SRC_DIR" || ! -d "$SRC_DIR/$GDAL_SRC_DIR" ]]; then
  "$SCRIPT_DIR/prepare-tree.sh"
fi

SQLITE_SRC="$SRC_DIR/$SQLITE_SRC_DIR"
if [[ ! -x "$SQLITE_SRC/configure" ]]; then
  echo "sqlite configure script not found: $SQLITE_SRC/configure" >&2
  exit 1
fi

SQLITE_BUILD="$BUILD_WORK_DIR/sqlite"
rm -rf "$SQLITE_BUILD"
mkdir -p "$SQLITE_BUILD"

cflags=("-O2" "-fPIC")
ldflags=()
append_macos_cflags cflags
append_macos_cflags ldflags

pushd "$SQLITE_BUILD" >/dev/null
CFLAGS="${cflags[*]}" LDFLAGS="${ldflags[*]}" \
  "$SQLITE_SRC/configure" \
  --prefix="$STAGE_DIR" \
  --disable-shared \
  --enable-static \
  --disable-readline
make -j"$JOBS"
make install
popd >/dev/null

if [[ ! -f "$(stage_lib libsqlite3.a)" ]]; then
  echo "sqlite static library missing after build" >&2
  exit 1
fi

echo "Built sqlite static: $(stage_lib libsqlite3.a)"
