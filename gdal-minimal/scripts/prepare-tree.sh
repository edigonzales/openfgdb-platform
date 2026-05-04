#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"

require_cmd tar find patch
load_versions
ensure_dirs

"$SCRIPT_DIR/fetch-sources.sh"

extract_archive_to "$GDAL_ARCHIVE" "$GDAL_SRC_DIR"
extract_archive_to "$PROJ_ARCHIVE" "$PROJ_SRC_DIR"
extract_archive_to "$SQLITE_ARCHIVE" "$SQLITE_SRC_DIR"

for patch_file in "$GDAL_MINIMAL_DIR"/patches/gdal-*.patch; do
  if [[ -f "$patch_file" ]]; then
    echo "Applying GDAL patch: $(basename "$patch_file")"
    patch -d "$SRC_DIR/$GDAL_SRC_DIR" -p1 --forward --batch < "$patch_file"
  fi
done

echo "Prepared source tree under: $SRC_DIR"
