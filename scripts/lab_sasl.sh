#!/usr/bin/env bash
# Lab exercise 3: SASL/SCRAM authentication and ACLs.
#   qm-api  may ONLY produce on qm.*
#   qm-sink may consume qm.* as group audit-sink, and write ONLY qm.dlq.v1 — the sink also
#           produces: rejected messages go to the DLQ (found in this exercise: a read-only sink
#           would fail on its first poison message).
# Runs against the `kafka-sasl` service (profile sasl) of the lab and prints every
# outcome. Exits non-zero if an authorisation result is not the expected one.
set -euo pipefail
cd "$(dirname "$0")/.."

LAB=(docker compose -f docker-compose.lab.yml --profile sasl)
K() {
  local tool="$1"
  shift
  "${LAB[@]}" exec -T kafka-sasl env KAFKA_HEAP_OPTS="-Xmx64m -Xms32m" "/opt/kafka/bin/$tool" "$@"
}
ADMIN=(--bootstrap-server localhost:9092)   # plaintext door, inside the container only
CLIENT=kafka-sasl:9095                       # the SASL door
FAIL=0
expect() { # LABEL EXPECT(ok|denied) COMMAND… → runs it, compares
  local label="$1" expect="$2" out rc=0
  shift 2
  out="$("$@" 2>&1)" || rc=$?
  local failed=0
  grep -qE 'AuthorizationException|Authentication failed|AUTHENTICATION|SaslAuthenticationException|TOPIC_AUTHORIZATION|GROUP_AUTHORIZATION|Not authorized|not authorized|UnsupportedSaslMechanism|Failed to authenticate|Timed out|TimeoutException|Disconnected' <<<"$out" && failed=1
  ((rc != 0)) && failed=1
  if [[ "$expect" == "ok" && $failed -eq 0 ]] || [[ "$expect" == "denied" && $failed -eq 1 ]]; then
    echo "  ✓ $label"
  else
    echo "  ✗ $label (expected $expect)"
    echo "$out" | grep -E 'Exception|rror|denied|Authorization|Authentication' | head -3 | sed 's/^/      /'
    FAIL=1
  fi
}

echo "→ starting kafka-sasl"
"${LAB[@]}" up -d kafka-sasl >/dev/null
for _ in $(seq 1 60); do
  K kafka-broker-api-versions.sh "${ADMIN[@]}" >/dev/null 2>&1 && break
  sleep 2
done

echo "→ users (SCRAM credentials live in the cluster metadata) and topics"
K kafka-configs.sh "${ADMIN[@]}" --alter --entity-type users --entity-name qm-api --add-config 'SCRAM-SHA-256=[iterations=8192,password=api-secret]'
K kafka-configs.sh "${ADMIN[@]}" --alter --entity-type users --entity-name qm-sink --add-config 'SCRAM-SHA-256=[iterations=8192,password=sink-secret]'
for t in qm.audit.valuation.v1 qm.dlq.v1 other.topic.v1; do
  K kafka-topics.sh "${ADMIN[@]}" --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null
done

echo "→ ACLs"
K kafka-acls.sh "${ADMIN[@]}" --add --allow-principal User:qm-api --operation Write --operation Describe --topic qm. --resource-pattern-type prefixed >/dev/null
K kafka-acls.sh "${ADMIN[@]}" --add --allow-principal User:qm-sink --operation Read --operation Describe --topic qm. --resource-pattern-type prefixed >/dev/null
K kafka-acls.sh "${ADMIN[@]}" --add --allow-principal User:qm-sink --operation Read --group audit-sink >/dev/null
K kafka-acls.sh "${ADMIN[@]}" --add --allow-principal User:qm-sink --operation Write --topic qm.dlq.v1 >/dev/null
K kafka-acls.sh "${ADMIN[@]}" --list

# client property files, written inside the container
for entry in "qm-api api-secret" "qm-sink sink-secret" "qm-api wrong-password" "nobody nobody-pw"; do
  read -r user pw <<<"$entry"
  "${LAB[@]}" exec -T kafka-sasl bash -c "cat > /tmp/$user-$pw.properties <<EOP
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$user\" password=\"$pw\";
EOP"
done

# shellcheck disable=SC2329  # called through expect()
produce() { echo "k|v" | K kafka-console-producer.sh --bootstrap-server "$CLIENT" --topic "$1" --producer.config "/tmp/$2.properties" --property parse.key=true --property 'key.separator=|' --producer-property max.block.ms=6000 --producer-property request.timeout.ms=4000 --producer-property delivery.timeout.ms=6000; }
# shellcheck disable=SC2329  # called through expect()
consume() { K kafka-console-consumer.sh --bootstrap-server "$CLIENT" --topic "$1" --group "$3" --from-beginning --max-messages 1 --timeout-ms 8000 --consumer.config "/tmp/$2.properties"; }

# shellcheck disable=SC2329  # called through expect()
produce_anonymous() { echo k | K kafka-console-producer.sh --bootstrap-server "$CLIENT" --topic qm.audit.valuation.v1 --producer-property max.block.ms=6000 --producer-property request.timeout.ms=4000; }

echo "→ what each identity can do"
expect "qm-api PRODUCES on qm.audit.valuation.v1" ok produce qm.audit.valuation.v1 qm-api-api-secret
expect "qm-api CANNOT consume qm.audit.valuation.v1" denied consume qm.audit.valuation.v1 qm-api-api-secret g-api
expect "qm-api CANNOT produce outside qm.* (other.topic.v1)" denied produce other.topic.v1 qm-api-api-secret
expect "qm-sink CONSUMES qm.audit.valuation.v1 as group audit-sink" ok consume qm.audit.valuation.v1 qm-sink-sink-secret audit-sink
expect "qm-sink CANNOT produce on the audit topics" denied produce qm.audit.valuation.v1 qm-sink-sink-secret
expect "qm-sink CAN write the DLQ (its own rejected messages)" ok produce qm.dlq.v1 qm-sink-sink-secret
expect "qm-sink CANNOT consume with another group" denied consume qm.audit.valuation.v1 qm-sink-sink-secret intruders
expect "a wrong password is refused" denied produce qm.audit.valuation.v1 qm-api-wrong-password
expect "an unknown user is refused" denied produce qm.audit.valuation.v1 nobody-nobody-pw
expect "no credentials at all is refused" denied produce_anonymous

exit "$FAIL"
