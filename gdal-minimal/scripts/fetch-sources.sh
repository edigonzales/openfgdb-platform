#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"

require_cmd curl awk tar
load_versions
ensure_dirs

download_archive "$GDAL_URL" "$GDAL_ARCHIVE"
download_archive "$PROJ_URL" "$PROJ_ARCHIVE"
download_archive "$SQLITE_URL" "$SQLITE_ARCHIVE"

echo "All source archives downloaded and verified."
