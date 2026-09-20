#!/usr/bin/env bash
# Smoke test: produce one valid envelope on qm.audit.valuation.v1, read it back,
# compare byte for byte. Also proves that auto-create is off (an unknown topic
# is refused). Exits 0 or non-zero. Needs the platform up (`docker compose up -d`).
#
# The event has type `smoke.test` and username `smoke`; once the audit sink runs
# (WP 02) it is also archived — the audit trail is append-only, so it stays.
set -euo pipefail
cd "$(dirname "$0")/.."

TOPIC="qm.audit.valuation.v1"
KEY="smoke"
KAFKA=(docker compose exec -T kafka)
BIN=/opt/kafka/bin
BOOTSTRAP=localhost:9092
# Each tool starts its own JVM inside the container: keep its heap small.
TOOLS_HEAP="-Xmx64m -Xms32m"

fail() {
  echo "✗ smoke: $*" >&2
  exit 1
}
kafka() { "${KAFKA[@]}" env KAFKA_HEAP_OPTS="$TOOLS_HEAP" "$@"; }

# UUID v7 (48-bit unix ms, then version/variant bits), as the contract requires.
event_id="$(python3 - <<'PY'
import os, time, uuid
ms = int(time.time() * 1000)
rand = int.from_bytes(os.urandom(10), "big")
value = (ms << 80) | (0x7 << 76) | ((rand >> 68) << 64) | (0b10 << 62) | (rand & ((1 << 62) - 1))
print(uuid.UUID(int=value))
PY
)"
now="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
trace_id="$(openssl rand -hex 16)"
sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
message="$(printf '{"event_id":"%s","type":"smoke.test","version":1,"occurred_at":"%s","request_id":"smoke","trace_id":"%s","username":"%s","producer":{"service":"smoke.sh","git_sha":"%s","lib_build":"none"},"payload":{}}' \
  "$event_id" "$now" "$trace_id" "$KEY" "$sha")"

# 1. Auto-create must be off: producing on an unknown topic has to fail. The
#    console producer exits 0 even when delivery fails, so read its error and
#    check that no ghost topic appeared.
GHOST="qm.smoke.does-not-exist.v1"
ghost_out="$(echo "k|{}" | kafka "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$GHOST" --property parse.key=true --property 'key.separator=|' \
  --producer-property max.block.ms=6000 --producer-property delivery.timeout.ms=6000 \
  --producer-property request.timeout.ms=3000 2>&1 || true)"
grep -q 'UnknownTopicOrPartition\|not present in metadata' <<<"$ghost_out" ||
  fail "producing on an unknown topic did not fail — is auto.create.topics.enable really off?"
kafka "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qxF "$GHOST" &&
  fail "a ghost topic $GHOST was created — auto.create.topics.enable is on"
echo "✓ unknown topic refused, no ghost topic (auto-create is off)"

# 2. Remember where each partition ends, so we read back only what we produce.
declare -A start
while IFS=: read -r _ part offset; do
  start["$part"]="$offset"
done < <(kafka "$BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
((${#start[@]})) || fail "topic $TOPIC not found — did topics-init run?"

# 3. Produce.
printf '%s|%s\n' "$KEY" "$message" | kafka "$BIN/kafka-console-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --property parse.key=true --property 'key.separator=|' \
  --producer-property acks=all >/dev/null
echo "✓ produced $event_id on $TOPIC"

# 4. Read back: the key hashes to one partition; ask each from its remembered end.
found=""
for part in "${!start[@]}"; do
  read_back="$(kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" --partition "$part" --offset "${start[$part]}" \
    --property print.key=true --property 'key.separator=|' \
    --timeout-ms 4000 2>/dev/null || true)"
  if grep -qF "$event_id" <<<"$read_back"; then
    found="$read_back"
    break
  fi
done
[[ -n "$found" ]] || fail "event $event_id was not read back from $TOPIC"

[[ "$(grep -F "$event_id" <<<"$found" | head -n1)" == "${KEY}|${message}" ]] ||
  fail "the message read back differs from the one produced"
echo "✓ read back identical (key=$KEY)"

# 5. If the audit sink is running (WP 02), the event must reach Postgres too.
if [[ -n "$(docker compose ps --status running --services 2>/dev/null | grep -x audit-sink || true)" ]]; then
  for _ in $(seq 1 30); do
    count="$(docker compose exec -T qm-audit psql -U qm_admin -d qm_audit -Atc \
      "SELECT count(*) FROM audit.events WHERE event_id = '$event_id'" 2>/dev/null || echo 0)"
    [[ "$count" == "1" ]] && break
    sleep 1
  done
  [[ "${count:-0}" == "1" ]] || fail "event $event_id never reached audit.events (sink → Postgres)"
  echo "✓ archived in audit.events by audit-sink"
fi

echo "✓ smoke test passed"
