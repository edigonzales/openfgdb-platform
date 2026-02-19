# GDAL Core Minimal Vendor Import

This directory stores vendored GDAL core headers and generated config/version
headers needed by the native OpenFileGDB integration in `openfgdb4j`.

## Source baseline

- Upstream: `https://github.com/OSGeo/gdal`
- Tag: `v3.12.0`
- Import script: `openfgdb4j/scripts/import-gdal-core-minimal.sh`

## Policy

- Keep imported files deterministic and listed in `IMPORTED_FILES.txt`.
- Mirror headers from `port`, `gcore`, and `ogr` for compile-time compatibility.
- Generate `port/cpl_config.h`, `gcore/gdal_version.h`, and
  `gcore/gdal_version_full/gdal_version.h` for macOS arm64.
- Do not rely on a system-wide GDAL runtime installation.
