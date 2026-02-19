# GDAL OpenFileGDB Vendor-Minimal Import

This directory contains the vendored OpenFileGDB source tree used by `openfgdb4j`.

## Source baseline

- Upstream: `https://github.com/OSGeo/gdal`
- Tag: `v3.12.0`
- Import script: `openfgdb4j/scripts/import-gdal-openfilegdb.sh`

## Policy

- Vendor the complete `ogr/ogrsf_frmts/openfilegdb` subtree from a fixed GDAL tag.
- Track imported files in `IMPORTED_FILES.txt`.
- Place local adaptations as patch files in `patches/`.
- Do not add a runtime dependency on a system GDAL installation.
