#!/usr/bin/env bash
# Apply the SQL migrations in order, then keep the monthly partitions in shape.
# Runs INSIDE a postgres image as `audit_owner` (compose service `audit-migrate`):
#
#   docker compose run --rm audit-migrate            # migrate + ensure partitions
#   docker compose run --rm audit-migrate maintain   # ... + drop expired partitions
#
# Rules:
#   - migrations/NNNN_name.sql are applied in name order, each in ONE transaction
#     (do not put BEGIN/COMMIT in them);
#   - applied versions are recorded in audit.schema_migrations with the sha256 of
#     the file. A migration that changed after being applied is an ERROR: never
#     edit an applied migration, add a new one;
#   - re-running applies nothing new.
# Environment: PGHOST PGDATABASE PGUSER PGPASSWORD, MIGRATIONS_DIR, PARTITIONS_BACK,
# PARTITIONS_AHEAD, RETENTION_MONTHS, DRY_RUN (1 = only report what would be dropped).
set -euo pipefail

MIGRATIONS_DIR="${MIGRATIONS_DIR:-/opt/migrate/migrations}"
PARTITIONS_BACK="${PARTITIONS_BACK:-3}"   # Kafka keeps at most 90 days: a replay can reach 3 months back
PARTITIONS_AHEAD="${PARTITIONS_AHEAD:-3}"
RETENTION_MONTHS="${RETENTION_MONTHS:-12}"
DRY_RUN="${DRY_RUN:-0}"
mode="${1:-migrate}"

# client_min_messages: hide the harmless "already exists, skipping" notices.
sql() { PGOPTIONS="-c client_min_messages=warning" psql -X -q -v ON_ERROR_STOP=1 "$@"; }

migrate() {
  sql <<'SQL'
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.schema_migrations (
    version    text        PRIMARY KEY,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
SQL
  local applied=0 file version checksum recorded
  for file in "$MIGRATIONS_DIR"/*.sql; do
    [[ -e "$file" ]] || break
    version="$(basename "$file" .sql)"
    checksum="$(sha256sum "$file" | cut -d' ' -f1)"
    recorded="$(sql -At -c "SELECT checksum FROM audit.schema_migrations WHERE version = '$version'")"
    if [[ -n "$recorded" ]]; then
      if [[ "$recorded" != "$checksum" ]]; then
        echo "✗ migration $version was modified after being applied — never edit an applied migration, add a new one" >&2
        exit 1
      fi
      continue
    fi
    if grep -qiE '^[[:space:]]*(BEGIN|COMMIT|ROLLBACK)[[:space:]]*;' "$file"; then
      echo "✗ $version contains transaction control; the runner already wraps each file in a transaction" >&2
      exit 1
    fi
    {
      echo "BEGIN;"
      echo "SELECT pg_advisory_xact_lock(727274) \\g /dev/null"
      cat "$file"
      echo "INSERT INTO audit.schema_migrations (version, checksum) VALUES ('$version', '$checksum');"
      echo "COMMIT;"
    } | sql
    echo "＋ applied $version"
    applied=$((applied + 1))
  done
  echo "migrations: applied=$applied"
}

migrate
sql -At -c "SELECT 'partitions created: ' || audit.ensure_partitions($PARTITIONS_BACK, $PARTITIONS_AHEAD)"

if [[ "$mode" == "maintain" ]]; then
  dry="false"
  [[ "$DRY_RUN" == "1" ]] && dry="true"
  sql -At -c "SELECT 'partitions dropped (dry_run=$dry): ' || coalesce(array_to_string(audit.drop_expired_partitions($RETENTION_MONTHS, $dry), ', '), '-')"
fi
