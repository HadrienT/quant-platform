#!/usr/bin/env bash
# Dump the audit database (pg_dump, custom format) to a path OUTSIDE the container.
# An audit trail that is not backed up is not an audit trail.
#
#   scripts/backup_audit.sh [DEST_DIR]      default: $BACKUP_DEST or ~/backups/quant-platform
#
# Keeps the newest $BACKUP_KEEP dumps (default 14). Each dump is verified readable
# (pg_restore --list) before older ones are pruned. Driven by the systemd timer
# deploy/quant-platform-backup.timer.
set -euo pipefail
cd "$(dirname "$0")/.."

DEST="${1:-${BACKUP_DEST:-$HOME/backups/quant-platform}}"
KEEP="${BACKUP_KEEP:-14}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
file="$DEST/audit-$stamp.dump"

umask 077
mkdir -p "$DEST"
tmp="$file.partial"
trap 'rm -f "$tmp"' EXIT

# Over the local socket as the bootstrap superuser: it can read every partition.
docker compose exec -T qm-audit pg_dump -U qm_admin -d qm_audit -Fc >"$tmp"

if ! docker compose exec -T qm-audit pg_restore --list <"$tmp" >/dev/null; then
  echo "✗ the dump is not readable — keeping the previous backups untouched" >&2
  exit 1
fi
mv "$tmp" "$file"
echo "✓ backup written: $file ($(du -h "$file" | cut -f1))"

# Prune: newest $KEEP stay.
mapfile -t old < <(find "$DEST" -maxdepth 1 -name 'audit-*.dump' | sort -r | tail -n +"$((KEEP + 1))")
if ((${#old[@]})); then
  rm -f "${old[@]}"
  echo "→ pruned ${#old[@]} old backup(s)"
fi
