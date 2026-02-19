#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
if [[ -n "${JEXTRACT_BIN:-}" ]]; then
  JEXTRACT_BIN="${JEXTRACT_BIN}"
else
  JEXTRACT_BIN="jextract"
fi

if ! command -v "$JEXTRACT_BIN" >/dev/null 2>&1; then
  echo "jextract not found: $JEXTRACT_BIN" >&2
  echo "Set JEXTRACT_BIN to a valid executable path." >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR/src/generated/java"

"$JEXTRACT_BIN" \
  --include-dir "$PROJECT_DIR/native/include" \
  --target-package ch.ehi.openfgdb4j.ffm \
  --header-class-name OpenFgdbNative \
  --output "$PROJECT_DIR/src/generated/java" \
  "$PROJECT_DIR/native/include/openfgdb_c_api.h"

# jextract may emit SymbolLookup#findOrThrow, which is not available on all installed JDKs.
while IFS= read -r -d '' file; do
  perl -0777 -i -pe 's/SYMBOL_LOOKUP\.findOrThrow\("([^"]+)"\)/SYMBOL_LOOKUP.find("$1").orElseThrow(() -> new IllegalStateException("Native symbol not found: $1"))/g' "$file"
done < <(find "$PROJECT_DIR/src/generated/java" -name '*.java' -print0)
