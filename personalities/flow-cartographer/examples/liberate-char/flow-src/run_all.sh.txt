#!/usr/bin/env bash
# Stage this (hardcoded-path) reference flow to its fixed root and run the
# mock liberate. The tcl/scr files contain literal /tmp/liberate-char-ref
# paths, so we stage there first, then run.
set -euo pipefail
REF_ROOT=/tmp/liberate-char-ref
HERE="$(cd "$(dirname "$0")" && pwd)"

rm -rf "$REF_ROOT"
mkdir -p "$REF_ROOT"
cp -r "$HERE/." "$REF_ROOT/"
chmod +x "$REF_ROOT/bin/liberate"

python3 "$REF_ROOT/bin/liberate" -scr "$REF_ROOT/run.scr"

echo "--- outputs in $REF_ROOT/out ---"
ls -1 "$REF_ROOT/out"
