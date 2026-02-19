# gdal-minimal

Deterministic build pipeline for a static minimal GDAL stack used by `openfgdb4j`.

## Scope

Build outputs (stage directory):

- SQLite static library
- PROJ static library (with embedded `proj.db` resources)
- GDAL static library (`OpenFileGDB` enabled, optional drivers disabled)

Supported build targets:

- Unix scripts (`*.sh`): Linux + macOS
- PowerShell script (`build-all.ps1`): Windows (MSVC)

## Layout

- `manifests/versions.lock`: pinned source versions and URLs.
- `manifests/SHA256SUMS`: archive checksums (mandatory verification).
- `scripts/*.sh`: Unix fetch/prepare/build orchestration.
- `scripts/build-all.ps1`: Windows MSVC orchestration.
- `build/stage`: install/staging output (ignored by git).
- `third_party/downloads`: downloaded tarballs (ignored by git).
- `third_party/src`: extracted source trees (ignored by git).

## Environment variables

- `OPENFGDB4J_TARGET_OS=linux|macos|windows`
- `OPENFGDB4J_TARGET_ARCH=amd64|arm64`
- `OPENFGDB4J_GDAL_MINIMAL_ROOT`: override stage directory path.
  - default: `gdal-minimal/build/stage`
- `OPENFGDB4J_GDAL_MINIMAL_REBUILD=1`: force clean rebuild.
- `OPENFGDB4J_GDAL_MINIMAL_JOBS`: parallel build jobs (Unix scripts).
- `OPENFGDB4J_CMAKE_GENERATOR`: CMake generator override.
- `CMAKE_BIN`: CMake executable path.

## Build

Unix:

```bash
gdal-minimal/scripts/build-all.sh
```

Windows:

```powershell
gdal-minimal/scripts/build-all.ps1
```

## Notes

- All archives are SHA256-verified before extraction.
- The pipeline is deterministic through `versions.lock` + `SHA256SUMS`.
- Runtime system libraries remain dynamic; third-party geospatial stack is statically linked into `openfgdb`.
