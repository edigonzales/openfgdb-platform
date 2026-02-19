#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"

require_cmd "$CMAKE_BIN"
load_versions
ensure_dirs

if [[ "${OPENFGDB4J_GDAL_MINIMAL_REBUILD:-0}" != "1" && -f "$(stage_lib libgdal.a)" ]]; then
  echo "gdal already built: $(stage_lib libgdal.a)"
  exit 0
fi

"$SCRIPT_DIR/build-proj.sh"

GDAL_SRC="$SRC_DIR/$GDAL_SRC_DIR"
if [[ ! -f "$GDAL_SRC/CMakeLists.txt" ]]; then
  echo "gdal source not prepared: $GDAL_SRC" >&2
  exit 1
fi

GDAL_BUILD="$BUILD_WORK_DIR/gdal"
rm -rf "$GDAL_BUILD"
mkdir -p "$GDAL_BUILD"

cmake_args=(
  -S "$GDAL_SRC"
  -B "$GDAL_BUILD"
  -G "$CMAKE_GENERATOR"
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON
  -DCMAKE_C_FLAGS=-fPIC
  -DCMAKE_CXX_FLAGS=-fPIC
  -DCMAKE_INSTALL_PREFIX="$STAGE_DIR"
  -DBUILD_SHARED_LIBS=OFF
  -DBUILD_APPS=OFF
  -DBUILD_TESTING=OFF
  -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF
  -DOGR_BUILD_OPTIONAL_DRIVERS=OFF
  -DOGR_ENABLE_DRIVER_OPENFILEGDB=ON
  -DGDAL_USE_EXTERNAL_LIBS=OFF
  -DGDAL_USE_INTERNAL_LIBS=ON
  -DGDAL_USE_ZLIB_INTERNAL=ON
  -DGDAL_USE_TIFF_INTERNAL=ON
  -DGDAL_USE_GEOTIFF_INTERNAL=ON
  -DGDAL_USE_JSONC_INTERNAL=ON
  -DGDAL_USE_LIBPNG_INTERNAL=ON
  -DGDAL_USE_CURL=OFF
  -DGDAL_USE_EXPAT=OFF
  -DPROJ_DIR="$STAGE_DIR/lib/cmake/proj"
  -DSQLite3_INCLUDE_DIR="$STAGE_DIR/include"
  -DSQLite3_LIBRARY="$STAGE_DIR/lib/libsqlite3.a"
)
append_macos_cmake_arch cmake_args

"$CMAKE_BIN" "${cmake_args[@]}"
"$CMAKE_BIN" --build "$GDAL_BUILD" --config Release -- -j"$JOBS"
"$CMAKE_BIN" --build "$GDAL_BUILD" --target install --config Release

if [[ ! -f "$(stage_lib libgdal.a)" ]]; then
  echo "gdal static library missing after build" >&2
  exit 1
fi

echo "Built gdal static: $(stage_lib libgdal.a)"
