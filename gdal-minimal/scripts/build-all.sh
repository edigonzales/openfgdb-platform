#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"

load_versions
ensure_dirs

if [[ "${OPENFGDB4J_GDAL_MINIMAL_REBUILD:-0}" == "1" ]]; then
  echo "Rebuild requested: clearing stage + work directories"
  rm -rf "$BUILD_WORK_DIR" "$STAGE_DIR"
  ensure_dirs
fi

if [[ "$TARGET_OS" == "windows" ]]; then
  echo "gdal-minimal/scripts/build-all.sh does not support Windows shell builds." >&2
  echo "Use gdal-minimal/scripts/build-all.ps1 for MSVC builds." >&2
  exit 1
fi

"$SCRIPT_DIR/prepare-tree.sh"
"$SCRIPT_DIR/build-sqlite.sh"
"$SCRIPT_DIR/build-proj.sh"
"$SCRIPT_DIR/build-gdal.sh"

echo "gdal-minimal build complete"
echo "  target: $TARGET_OS/$TARGET_ARCH"
echo "  stage: $STAGE_DIR"
