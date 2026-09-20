# shellcheck shell=bash
# Helpers shared by the integration scripts (crash_test.sh, audit_e2e.sh, …).
# Source it; it expects to run from the repository root against a running stack.

TOPIC_VALUATION="qm.audit.valuation.v1"

# Run with a small heap: each Kafka tool starts its own JVM inside the 1 GiB broker container.
kafka_tool() {
  local tool="$1"
  shift
  docker compose exec -T kafka env KAFKA_HEAP_OPTS="-Xmx64m -Xms32m" \
    "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"
}

# SQL as the bootstrap superuser, over the local socket (never over TCP).
sql_admin() {
  docker compose exec -T qm-audit psql -U qm_admin -d qm_audit -X -At -v ON_ERROR_STOP=1 "$@"
}

# produce_events COUNT TAG [gen_events.py options…] → onto the valuation topic
produce_events() {
  python3 scripts/gen_events.py "$@" | docker compose exec -T kafka env KAFKA_HEAP_OPTS="-Xmx64m -Xms32m" \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic "$TOPIC_VALUATION" --property parse.key=true --property 'key.separator=|' \
    --producer-property acks=all >/dev/null
}

# produce_file TOPIC FILE — a file of `key|json` lines onto any topic
produce_file() {
  docker compose exec -T kafka env KAFKA_HEAP_OPTS="-Xmx64m -Xms32m" \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic "$1" --property parse.key=true --property 'key.separator=|' \
    --producer-property acks=all <"$2" >/dev/null
}

# group_lag GROUP [TOPIC] — total unread messages of a consumer group (0 when caught up)
group_lag() {
  kafka_tool kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null |
    awk -v topic="${2:-}" 'NR > 1 && $2 != "" && (topic == "" || $2 == topic) && $6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}'
}

rows_for() { sql_admin -c "SELECT count(*) FROM audit.events WHERE username = '$1'"; }
distinct_for() { sql_admin -c "SELECT count(DISTINCT event_id) FROM audit.events WHERE username = '$1'"; }

# wait_for_rows TAG EXPECTED [TIMEOUT_S]
wait_for_rows() {
  local tag="$1" expected="$2" timeout="${3:-120}" n=0 i
  for ((i = 0; i < timeout; i++)); do
    n="$(rows_for "$tag" 2>/dev/null || echo 0)"
    [[ "$n" == "$expected" ]] && return 0
    sleep 1
  done
  echo "timeout: $n rows for $tag, expected $expected" >&2
  return 1
}

# Messages ever written to a topic (sum of end offsets — Kafka has no delete here).
topic_end_offsets() {
  kafka_tool kafka-get-offsets.sh --topic "$1" --time -1 | awk -F: '{s += $3} END {print s + 0}'
}

PASS=0
FAIL=0
ok() {
  PASS=$((PASS + 1))
  echo "  ✓ $*"
}
ko() {
  FAIL=$((FAIL + 1))
  echo "  ✗ $*" >&2
}
finish() {
  echo
  echo "$PASS passed, $FAIL failed"
  [[ "$FAIL" -eq 0 ]]
}
