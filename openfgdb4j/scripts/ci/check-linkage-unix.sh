#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: check-linkage-unix.sh <native-library>" >&2
  exit 1
fi

LIB_PATH="$1"
if [[ ! -f "$LIB_PATH" ]]; then
  echo "Library not found: $LIB_PATH" >&2
  exit 1
fi

uname_s="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$uname_s" == darwin* ]]; then
  OUT="$(otool -L "$LIB_PATH")"
  echo "$OUT"
  if echo "$OUT" | grep -Eiq 'libgdal(\.[0-9]+)?\.dylib'; then
    echo "Unexpected dynamic gdal dependency detected in $LIB_PATH" >&2
    exit 1
  fi
elif [[ "$uname_s" == linux* ]]; then
  OUT="$(ldd "$LIB_PATH")"
  echo "$OUT"
  if echo "$OUT" | grep -Eiq 'libgdal(\.so(\.[0-9]+)*)?'; then
    echo "Unexpected dynamic gdal dependency detected in $LIB_PATH" >&2
    exit 1
  fi
else
  echo "Unsupported OS for linkage check: $uname_s" >&2
  exit 1
fi
