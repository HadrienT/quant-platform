#!/usr/bin/env bash
# Export the schema registry (every schema, every version, with its id) to a zip
# next to the audit dumps. The registry's live state is a never-expiring Kafka
# topic; this export is the second copy, and the way to rebuild it elsewhere.
#
#   scripts/backup_registry.sh [DEST_DIR]      default: $BACKUP_DEST or ~/backups/quant-platform
set -euo pipefail
cd "$(dirname "$0")/.."

DEST="${1:-${BACKUP_DEST:-$HOME/backups/quant-platform}}"
KEEP="${BACKUP_KEEP:-14}"
file="$DEST/registry-$(date -u +%Y%m%dT%H%M%SZ).zip"

umask 077
mkdir -p "$DEST"
tmp="$file.partial"
trap 'rm -f "$tmp"' EXIT

docker compose exec -T schema-registry curl -sf http://localhost:8080/apis/registry/v3/admin/export >"$tmp"
if ! python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).testzip()' "$tmp"; then
  echo "✗ the registry export is not a valid zip — keeping previous backups" >&2
  exit 1
fi
mv "$tmp" "$file"
echo "✓ registry export written: $file ($(du -h "$file" | cut -f1))"

mapfile -t old < <(find "$DEST" -maxdepth 1 -name 'registry-*.zip' | sort -r | tail -n +"$((KEEP + 1))")
if ((${#old[@]})); then
  rm -f "${old[@]}"
fi
