#!/usr/bin/env bash
# Restore a dump into a SEPARATE database (default qm_audit_restore) inside the
# running qm-audit container, to verify a backup or to extract data. It refuses to
# touch the live database `qm_audit`: replacing the live trail is a manual,
# deliberate procedure (docs/RUNBOOK.md §5).
#
#   scripts/restore_audit.sh DUMP_FILE [TARGET_DB]
set -euo pipefail
cd "$(dirname "$0")/.."

dump="${1:?usage: restore_audit.sh DUMP_FILE [TARGET_DB]}"
target="${2:-qm_audit_restore}"
[[ -f "$dump" ]] || {
  echo "✗ no such file: $dump" >&2
  exit 1
}
if [[ "$target" == "qm_audit" ]]; then
  echo "✗ refusing to restore over the live database qm_audit (see docs/RUNBOOK.md §5)" >&2
  exit 1
fi

psql_admin() { docker compose exec -T qm-audit psql -U qm_admin -X -q -v ON_ERROR_STOP=1 "$@"; }

psql_admin -d postgres -c "DROP DATABASE IF EXISTS \"$target\"" -c "CREATE DATABASE \"$target\" OWNER audit_owner"
# --no-owner: objects belong to the restoring superuser; roles/grants come from the dump's ACLs.
docker compose exec -T qm-audit pg_restore -U qm_admin -d "$target" --no-owner <"$dump"
rows="$(psql_admin -d "$target" -At -c 'SELECT count(*) FROM audit.events')"
echo "✓ restored into database $target: $rows events"
