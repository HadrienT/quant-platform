#!/usr/bin/env bash
# Migrations and partition maintenance, on a THROWAWAY Postgres (own network, own
# container, removed at the end) — never on the running audit database.
#
#   - migrations on an empty database give the expected schema;
#   - replaying applies nothing; a migration edited after being applied is refused;
#   - retention drops WHOLE monthly partitions (dry-run first) and keeps the rest;
#   - a partition that cannot be created because DEFAULT holds its events warns.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

IMAGE="postgres:17.6"
NET="qp-migtest-net"
DB="qp-migtest-pg"
ADMIN_PW="admin-pw"
WORK=""

cleanup() {
  docker rm -f "$DB" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  [[ -z "$WORK" ]] || rm -rf "$WORK"
}
trap cleanup EXIT
cleanup
WORK="$(mktemp -d)"

docker network create "$NET" >/dev/null
docker run -d --name "$DB" --network "$NET" \
  -e POSTGRES_USER=qm_admin -e POSTGRES_PASSWORD="$ADMIN_PW" -e POSTGRES_DB=qm_audit \
  -e AUDIT_DB_OWNER_PASSWORD=o -e AUDIT_DB_WRITER_PASSWORD=w -e AUDIT_DB_READER_PASSWORD=r \
  -v "$PWD/qm-audit/init:/docker-entrypoint-initdb.d:ro" "$IMAGE" >/dev/null
for _ in $(seq 1 60); do
  # The image restarts the server after init: wait for the real one (TCP, not the temporary socket-only one).
  docker exec "$DB" pg_isready -h 127.0.0.1 -U qm_admin -d qm_audit >/dev/null 2>&1 && break
  sleep 1
done
sleep 2

# migrate [extra docker args…] -- [args of migrate.sh…]
migrate() {
  local dir="$PWD/migrations"
  if [[ "${1:-}" == "--dir" ]]; then
    dir="$2"
    shift 2
  fi
  docker run --rm --network "$NET" --entrypoint /bin/bash \
    -e PGHOST="$DB" -e PGDATABASE=qm_audit -e PGUSER=audit_owner -e PGPASSWORD=o \
    -e RETENTION_MONTHS=12 -e DRY_RUN="${DRY_RUN:-0}" \
    -v "$PWD/scripts/migrate.sh:/opt/migrate/migrate.sh:ro" -v "$dir:/opt/migrate/migrations:ro" \
    "$IMAGE" /opt/migrate/migrate.sh "$@" 2>&1
}
# DDL must run as the owner, exactly like the migrations (objects created by the superuser would not be droppable by it).
owner() { docker exec -i -e PGPASSWORD=o "$DB" psql -h 127.0.0.1 -U audit_owner -d qm_audit -X -At -v ON_ERROR_STOP=1 "$@"; }
# expect LABEL ACTUAL EXPECTED
expect() { if [[ "$2" == "$3" ]]; then ok "$1"; else ko "$1 (got '$2', expected '$3')"; fi; }
# expect_grep LABEL PATTERN TEXT
expect_grep() { if grep -q "$2" <<<"$3"; then ok "$1"; else ko "$1 ($3)"; fi; }
admin() { docker exec -i "$DB" psql -U qm_admin -d qm_audit -X -At -v ON_ERROR_STOP=1 "$@"; }

echo "1. Migrations on an empty database"
out="$(migrate)"
n_files="$(find migrations -name '*.sql' | wc -l)"
if grep -q "migrations: applied=$n_files" <<<"$out"; then ok "all $n_files migrations applied"; else ko "unexpected: $out"; fi
if grep -q "partitions created: 7" <<<"$out"; then ok "7 monthly partitions created (3 back, current, 3 ahead)"; else ko "partitions: $out"; fi
expect "5 views" "$(admin -c "SELECT count(*) FROM pg_views WHERE schemaname = 'audit'")" "5"
expect "3 non-superuser roles" "$(admin -c "SELECT count(*) FROM pg_roles WHERE rolname IN ('audit_owner','audit_writer','audit_reader') AND NOT rolsuper")" "3"
expect "applied versions recorded" "$(admin -c "SELECT count(*) FROM audit.schema_migrations")" "$n_files"

echo "2. Replay"
out="$(migrate)"
expect_grep "replay applies nothing" "migrations: applied=0" "$out"
expect_grep "replay creates no partition" "partitions created: 0" "$out"

echo "3. A migration edited after being applied is refused"
cp migrations/*.sql "$WORK/"
echo "-- tampered" >>"$WORK/0001_schema.sql"
if out="$(migrate --dir "$WORK")"; then ko "tampered migration was accepted"; elif grep -q "modified after being applied" <<<"$out"; then ok "refused, with an explanation"; else ko "failed for another reason: $out"; fi

echo "4. Retention drops whole partitions only"
owner -c "CREATE TABLE audit.events_y2023m01 PARTITION OF audit.events FOR VALUES FROM ('2023-01-01 00:00:00+00') TO ('2023-02-01 00:00:00+00')" >/dev/null
admin -c "INSERT INTO audit.events (event_id, type, version, occurred_at, producer, payload, src_topic, src_part, src_offset) VALUES (gen_random_uuid(), 'test.old', 1, '2023-01-15 12:00:00+00', '{}', '{}', 't', 0, 0)" >/dev/null
out="$(DRY_RUN=1 migrate maintain)"
expect_grep "dry-run names the expired partition" "dry_run=true): events_y2023m01" "$out"
expect "dry-run deleted nothing" "$(admin -c "SELECT count(*) FROM audit.events WHERE type = 'test.old'")" "1"
out="$(migrate maintain)"
expect_grep "the expired partition was dropped" "dry_run=false): events_y2023m01" "$out"
expect "the 7 current partitions and DEFAULT remain" "$(admin -c "SELECT count(*) FROM pg_inherits WHERE inhparent = 'audit.events'::regclass")" "8"
expect "the table is gone (no row-by-row DELETE involved)" "$(admin -c "SELECT to_regclass('audit.events_y2023m01') IS NULL")" "t"

echo "5. A month that DEFAULT already holds cannot get its partition: warn, do not fail"
admin -c "INSERT INTO audit.events (event_id, type, version, occurred_at, producer, payload, src_topic, src_part, src_offset) VALUES (gen_random_uuid(), 'test.future', 1, (date_trunc('month', now()) + interval '5 months' + interval '3 days'), '{}', '{}', 't', 0, 0)" >/dev/null
expect "the event is visible in v_default_partition_events" "$(admin -c "SELECT count(*) FROM audit.v_default_partition_events")" "1"
out="$(owner -c "SELECT audit.ensure_partitions(0, 5)" 2>&1 || true)"
expect_grep "warning names the cause instead of failing" "DEFAULT partition already holds" "$out"

echo "6. Views answer the audit questions (synthetic events)"
ev() { # ev TYPE USERNAME PAYLOAD_JSON
  admin -c "INSERT INTO audit.events (event_id, type, version, occurred_at, username, producer, payload, src_topic, src_part, src_offset) VALUES (gen_random_uuid(), '$1', 1, now(), '$2', '{}', '$3', 't', 0, 0)" >/dev/null
}
ev data.fallback u '{"kind":"default_rate"}'
ev data.fallback u '{"kind":"default_rate"}'
ev data.fallback u '{"kind":"stale_data"}'
ev auth.login_failed alice '{"ip_hash":"h1"}'
ev auth.login_failed bob '{"ip_hash":"h1"}'
ev auth.login_failed bob '{"ip_hash":"h2"}'
ev pricing.valuation u '{"product":"autocall","engine":{"name":"mc"},"model":{"name":"heston"},"timing":{"duration_ms":412},"market_inputs":[{"status":"observed"},{"status":"default"}]}'
ev pricing.valuation u '{"product":"vanilla","engine":{"name":"analytic"},"model":{"name":"bs"},"timing":{"duration_ms":3},"market_inputs":[{"status":"observed"},{"status":"stale"}]}'
ev pricing.valuation u '{"product":"vanilla","engine":{"name":"analytic"},"model":{"name":"bs"},"timing":{"duration_ms":"oops"},"market_inputs":[{"status":"observed"},{"status":"proxied"},{"status":"stale"}]}'
ev pricing.valuation u '{"product":"vanilla","engine":{"name":"analytic"},"model":{"name":"bs"},"timing":{"duration_ms":5},"market_inputs":[{"status":"observed"}]}'
expect "v_fallbacks_daily counts per kind" "$(admin -c "SELECT string_agg(kind || '=' || events, ',' ORDER BY kind) FROM audit.v_fallbacks_daily")" "default_rate=2,stale_data=1"
expect "v_login_failures_by_hash groups by hashed IP" "$(admin -c "SELECT string_agg(ip_hash || ':' || failures || ':' || distinct_usernames, ',' ORDER BY ip_hash) FROM audit.v_login_failures_by_hash")" "h1:2:2,h2:1:1"
expect "v_slowest_valuations orders by duration and skips garbage" "$(admin -c "SELECT string_agg(product || ':' || duration_ms, ',' ORDER BY duration_ms DESC) FROM audit.v_slowest_valuations")" "autocall:412,vanilla:5,vanilla:3"
expect "v_valuations_by_status: default/proxied = unobserved" "$(admin -c "SELECT string_agg(status || '=' || valuations, ',' ORDER BY status) FROM audit.v_valuations_by_status")" "observed=1,stale=1,unobserved=2"

echo "7. Hash chain: intact, then tampering is detected (0005)"
ins() { # ins OFFSET [PAYLOAD_JSON] — one event of partition (chain-test, 0), inserted as the WRITER would
  local payload="${2-}"
  [[ -n "$payload" ]] || payload="{\"n\":$1}"
  docker exec -i -e PGPASSWORD=w "$DB" psql -h 127.0.0.1 -U audit_writer -d qm_audit -X -q -v ON_ERROR_STOP=1 -c "INSERT INTO audit.events (event_id, type, version, occurred_at, producer, payload, src_topic, src_part, src_offset) VALUES ('00000000-0000-7000-8000-00000000000$1', 'test.chain', 1, '2026-09-01 10:00:0$1+00', '{}', '$payload', 'chain-test', 0, $1) ON CONFLICT DO NOTHING"
}
verify() { docker exec -i -e PGPASSWORD=r "$DB" psql -h 127.0.0.1 -U audit_reader -d qm_audit -X -At -F '|' -c "SELECT src_part, rows_checked, coalesce(broken_at::text, 'ok'), coalesce(reason, '') FROM audit.verify_chain('chain-test')"; }
for i in 0 1 2 3 4; do ins "$i"; done
expect "5 rows chained; the writer needed only INSERT" "$(verify)" "0|5|ok|"
ins 2 '{"n":"a re-delivered duplicate"}'
expect "a re-delivered event leaves the chain untouched" "$(verify)" "0|5|ok|"
part="$(admin -c "SELECT tableoid::regclass FROM audit.events WHERE src_topic = 'chain-test' AND src_offset = 2")"
admin -c "ALTER TABLE $part DISABLE TRIGGER events_append_only; UPDATE $part SET payload = '{\"n\":\"tampered\"}' WHERE src_topic = 'chain-test' AND src_offset = 2; ALTER TABLE $part ENABLE TRIGGER events_append_only" >/dev/null
expect "an altered row is found at its offset" "$(verify)" "0|5|2|row content was altered"
admin -c "ALTER TABLE $part DISABLE TRIGGER events_append_only; UPDATE $part SET payload = '{\"n\":2}' WHERE src_topic = 'chain-test' AND src_offset = 2; DELETE FROM $part WHERE src_topic = 'chain-test' AND src_offset = 3; ALTER TABLE $part ENABLE TRIGGER events_append_only" >/dev/null
expect "a row removed from the middle is found at the next offset" "$(verify)" "0|4|4|a row before this one was removed or altered"

finish
