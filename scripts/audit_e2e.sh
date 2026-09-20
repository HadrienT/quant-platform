#!/usr/bin/env bash
# End-to-end scenarios of the audit sink (blueprint WP 02 acceptance criteria),
# against a RUNNING dev stack. The audit trail is append-only, so the test rows it
# creates stay: use a disposable stack (`docker compose down -v` resets it).
#
#   scripts/audit_e2e.sh
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

trap 'docker compose up -d --no-deps qm-audit audit-sink >/dev/null 2>&1 || true' EXIT

echo "1. Poison message → DLQ, and the sink keeps consuming behind it"
TAG="dlq-$(openssl rand -hex 3)"
before="$(topic_end_offsets qm.dlq.v1)"
printf '%s|{this is not json\n' "$TAG" | docker compose exec -T kafka env KAFKA_HEAP_OPTS="-Xmx64m" \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$TOPIC_VALUATION" \
  --property parse.key=true --property 'key.separator=|' >/dev/null
produce_events 5 "$TAG"
if wait_for_rows "$TAG" 5 60; then ok "5 valid events behind the corrupt one were archived"; else ko "sink stalled behind the poison message"; fi
after="$(topic_end_offsets qm.dlq.v1)"
if ((after == before + 1)); then ok "exactly one message reached qm.dlq.v1"; else ko "DLQ grew by $((after - before)), expected 1"; fi
headers="$(kafka_tool kafka-console-consumer.sh --topic qm.dlq.v1 --from-beginning --timeout-ms 5000 \
  --property print.headers=true 2>/dev/null | grep -F '{this is not json' | tail -n1 || true)"
for h in dlq.source.topic dlq.source.partition dlq.source.offset dlq.error dlq.consumer.group; do
  if grep -q "$h:" <<<"$headers"; then ok "header $h present"; else ko "header $h missing"; fi
done

echo "2. Database outage → nothing in the DLQ, all events inserted after recovery, no duplicate"
TAG="outage-$(openssl rand -hex 3)"
before="$(topic_end_offsets qm.dlq.v1)"
docker compose stop qm-audit >/dev/null
produce_events 50 "$TAG"
sleep 10
after="$(topic_end_offsets qm.dlq.v1)"
if ((after == before)); then ok "no event was dead-lettered during the outage"; else ko "DLQ grew by $((after - before)) during the outage"; fi
docker compose up -d --wait qm-audit >/dev/null
if wait_for_rows "$TAG" 50 120; then ok "50 events inserted after the database came back"; else ko "events missing after recovery"; fi
if [[ "$(distinct_for "$TAG")" == "50" ]]; then ok "no duplicate"; else ko "duplicates after recovery"; fi

echo "3. Wrongly-dated event → DEFAULT partition, the sink does not stop"
TAG="olddate-$(openssl rand -hex 3)"
produce_events 1 "$TAG" --occurred-at 2019-05-05T00:00:00Z
produce_events 3 "$TAG-after"
if wait_for_rows "$TAG" 1 60; then ok "the misdated event was archived"; else ko "the misdated event was not archived"; fi
if wait_for_rows "$TAG-after" 3 60; then ok "the sink kept going"; else ko "the sink stopped"; fi
part="$(sql_admin -c "SELECT tableoid::regclass FROM audit.events WHERE username = '$TAG'")"
if [[ "$part" == "audit.events_default" ]]; then ok "it sits in audit.events_default"; else ko "it landed in $part"; fi

echo "4. SIGTERM → finish the batch, commit, exit 0"
docker compose stop -t 30 audit-sink >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' audit-sink)"
if [[ "$code" == "0" ]]; then ok "clean exit (code 0)"; else ko "exit code $code"; fi
if docker compose logs audit-sink 2>/dev/null | grep -q '"sink stopped"'; then ok "logged a clean stop"; else ko "no clean stop logged"; fi
docker compose up -d --no-deps audit-sink >/dev/null

echo "5. Rebuild: empty the table, reset the group's offsets, replay from Kafka"
sleep 5
docker compose stop audit-sink >/dev/null
before_n="$(sql_admin -c 'SELECT count(*) FROM audit.events')"
before_hash="$(sql_admin -c "SELECT md5(string_agg(event_id::text, ',' ORDER BY event_id)) FROM audit.events")"
sql_admin -c 'TRUNCATE audit.events' >/dev/null
if [[ "$(sql_admin -c 'SELECT count(*) FROM audit.events')" == "0" ]]; then ok "table emptied ($before_n rows)"; else ko "TRUNCATE failed"; fi
if kafka_tool kafka-consumer-groups.sh --group audit-sink --reset-offsets --to-earliest --all-topics --execute >/dev/null 2>&1; then
  ok "group offsets reset to earliest"
else
  ko "offset reset failed"
fi
docker compose up -d --no-deps audit-sink >/dev/null
for _ in $(seq 1 120); do
  [[ "$(sql_admin -c 'SELECT count(*) FROM audit.events')" == "$before_n" ]] && break
  sleep 1
done
after_n="$(sql_admin -c 'SELECT count(*) FROM audit.events')"
after_hash="$(sql_admin -c "SELECT md5(string_agg(event_id::text, ',' ORDER BY event_id)) FROM audit.events")"
if [[ "$after_n" == "$before_n" ]]; then ok "the table is back to $after_n rows"; else ko "rebuilt $after_n rows, expected $before_n"; fi
if [[ "$after_hash" == "$before_hash" ]]; then ok "same set of event_ids as before"; else ko "the rebuilt content differs"; fi

finish
