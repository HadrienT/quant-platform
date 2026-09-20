#!/usr/bin/env bash
# Privilege test (blueprint WP 02): nobody can UPDATE or DELETE audit.events, the
# reader cannot write, the writer can only INSERT. Every check connects OVER THE
# NETWORK with the role's real password, as the sink or Grafana would.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

env_value() { grep -E "^$1=" .env | head -n1 | cut -d= -f2-; }
OWNER_PW="$(env_value AUDIT_DB_OWNER_PASSWORD)"
WRITER_PW="$(env_value AUDIT_DB_WRITER_PASSWORD)"
READER_PW="$(env_value AUDIT_DB_READER_PASSWORD)"
ADMIN_PW="$(env_value AUDIT_DB_ADMIN_PASSWORD)"

# as ROLE PASSWORD SQL → prints psql's combined output; returns psql's exit code
as() {
  docker compose run --rm --no-deps --entrypoint psql \
    -e "PGUSER=$1" -e "PGPASSWORD=$2" audit-migrate -X -q -v ON_ERROR_STOP=1 -c "$3" 2>&1
}

INSERT_ROW="INSERT INTO audit.events (event_id, type, version, occurred_at, producer, payload, src_topic, src_part, src_offset)
            VALUES (gen_random_uuid(), 'test.priv', 1, now(), '{}', '{}', 't', 0, 0)"

# expect ROLE PW LABEL PATTERN SQL — SQL must FAIL with an error matching PATTERN
expect_error() {
  local role="$1" pw="$2" label="$3" pattern="$4" statement="$5" out rc=0
  out="$(as "$role" "$pw" "$statement")" || rc=$?
  if ((rc != 0)) && grep -qiE "$pattern" <<<"$out"; then ok "$label"; else ko "$label (rc=$rc: $out)"; fi
}
# expect_success ROLE PW LABEL SQL
expect_success() {
  local role="$1" pw="$2" label="$3" statement="$4" out rc=0
  out="$(as "$role" "$pw" "$statement")" || rc=$?
  if ((rc == 0)); then ok "$label"; else ko "$label ($out)"; fi
}

echo "audit_writer (INSERT only)"
expect_success audit_writer "$WRITER_PW" "INSERT is allowed (rolled back)" "BEGIN; $INSERT_ROW; ROLLBACK"
expect_success audit_writer "$WRITER_PW" "INSERT ... ON CONFLICT DO NOTHING is allowed" "BEGIN; $INSERT_ROW ON CONFLICT DO NOTHING; ROLLBACK"
expect_error audit_writer "$WRITER_PW" "UPDATE is refused" "permission denied" "UPDATE audit.events SET type = 'x'"
expect_error audit_writer "$WRITER_PW" "DELETE is refused" "permission denied" "DELETE FROM audit.events"
expect_error audit_writer "$WRITER_PW" "TRUNCATE is refused" "permission denied" "TRUNCATE audit.events"
expect_error audit_writer "$WRITER_PW" "SELECT is refused" "permission denied" "SELECT count(*) FROM audit.events"
expect_error audit_writer "$WRITER_PW" "DROP TABLE is refused" "must be owner|permission denied" "DROP TABLE audit.events"
expect_error audit_writer "$WRITER_PW" "creating objects in audit is refused" "permission denied" "CREATE TABLE audit.evil (a int)"
expect_error audit_writer "$WRITER_PW" "maintenance functions are not executable" "permission denied" "SELECT audit.ensure_partitions(0, 1)"

echo "audit_reader (SELECT only)"
expect_success audit_reader "$READER_PW" "SELECT on audit.events" "SELECT count(*) FROM audit.events"
expect_success audit_reader "$READER_PW" "SELECT on every view" \
  "SELECT (SELECT count(*) FROM audit.v_fallbacks_daily), (SELECT count(*) FROM audit.v_login_failures_by_hash), (SELECT count(*) FROM audit.v_slowest_valuations), (SELECT count(*) FROM audit.v_valuations_by_status), (SELECT count(*) FROM audit.v_default_partition_events)"
expect_error audit_reader "$READER_PW" "INSERT is refused" "permission denied" "$INSERT_ROW"
expect_error audit_reader "$READER_PW" "UPDATE is refused" "permission denied" "UPDATE audit.events SET type = 'x'"
expect_error audit_reader "$READER_PW" "DELETE is refused" "permission denied" "DELETE FROM audit.events"
expect_error audit_reader "$READER_PW" "TRUNCATE is refused" "permission denied" "TRUNCATE audit.events"

echo "audit_owner (DDL, but the trail is append-only for it too)"
expect_error audit_owner "$OWNER_PW" "UPDATE is refused by the trigger" "append-only" \
  "BEGIN; $INSERT_ROW; UPDATE audit.events SET type = 'x' WHERE type = 'test.priv'; ROLLBACK"
expect_error audit_owner "$OWNER_PW" "DELETE is refused by the trigger" "append-only" \
  "BEGIN; $INSERT_ROW; DELETE FROM audit.events WHERE type = 'test.priv'; ROLLBACK"

echo "bootstrap superuser (local socket only)"
if out="$(sql_admin -c "BEGIN; $INSERT_ROW; UPDATE audit.events SET type = 'x' WHERE type = 'test.priv'; ROLLBACK" 2>&1)" ||
  ! grep -q 'append-only' <<<"$out"; then
  ko "superuser UPDATE was not stopped by the trigger ($out)"
else
  ok "even a superuser's UPDATE is refused by the trigger"
fi
expect_error qm_admin "$ADMIN_PW" "the superuser is rejected over TCP" "pg_hba.conf rejects|no pg_hba" "SELECT 1"

echo "authentication"
expect_error audit_writer "wrong-password" "a wrong password is refused" "password authentication failed" "SELECT 1"

finish
