#!/usr/bin/env bash
# Lab exercise 6: lose a broker's disk. Same accident, two architectures:
#   - 3 brokers, replication factor 3: destroy kafka-3's volume, restart it;
#   - ONE broker, replication factor 1 (ADR-008, what the platform runs today).
# Needs: scripts/lab_up.sh solo
set -euo pipefail
cd "$(dirname "$0")/.."

LAB=(docker compose -f docker-compose.lab.yml --profile solo)
PROJECT=quant-platform-lab
count() { # SERVICE BOOTSTRAP TOPIC
  "${LAB[@]}" exec -T "$1" env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "$2" --topic "$3" --time -1 2>/dev/null | awk -F: '{s += $3} END {print s + 0}'
}
produce() { # SERVICE BOOTSTRAP TOPIC N
  seq 1 "$4" | sed 's/.*/k&|v&/' | "${LAB[@]}" exec -T "$1" env KAFKA_HEAP_OPTS="-Xmx64m" \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$2" --topic "$3" --property parse.key=true \
    --property 'key.separator=|' --producer-property acks=all >/dev/null 2>&1
}
wait_ready() { # SERVICE BOOTSTRAP
  for _ in $(seq 1 60); do
    "${LAB[@]}" exec -T "$1" env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$2" >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

echo "### A. 3 brokers, RF=3, min.insync.replicas=2"
CL=kafka-1:9092,kafka-2:9092,kafka-3:9092
"${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$CL" \
  --delete --if-exists --topic ex6 >/dev/null 2>&1 || true # start from an empty topic so the counts are exact
sleep 3
"${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$CL" \
  --create --if-not-exists --topic ex6 --partitions 3 --replication-factor 3 >/dev/null
produce kafka-1 "$CL" ex6 300
echo "  messages before the accident: $(count kafka-1 "$CL" ex6)"
"${LAB[@]}" rm -sf kafka-3 >/dev/null 2>&1  # stop AND remove: a volume in use cannot be deleted
docker volume rm "${PROJECT}_kafka-3-data" >/dev/null
echo "  kafka-3 stopped and its volume DELETED (a dead disk)"
"${LAB[@]}" up -d kafka-3 >/dev/null 2>&1
wait_ready kafka-1 "$CL"
for _ in $(seq 1 60); do
  isr="$("${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$CL" --describe --topic ex6 2>/dev/null | grep -c 'Isr: [0-9],[0-9],[0-9]' || true)"
  ((isr == 3)) && break
  sleep 3
done
echo "  after restarting kafka-3 on an EMPTY disk:"
"${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$CL" --describe --topic ex6 2>/dev/null | grep 'Partition:' | sed -E 's/\tElr.*//; s/^/    /'
echo "  messages after: $(count kafka-1 "$CL" ex6)   (kafka-3 re-copied its replicas from the other two)"

echo
echo "### B. ONE broker, RF=1 (what the platform runs today)"
"${LAB[@]}" up -d kafka-solo >/dev/null 2>&1
wait_ready kafka-solo kafka-solo:9092
"${LAB[@]}" exec -T kafka-solo env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-solo:9092 \
  --create --if-not-exists --topic ex6 --partitions 3 --replication-factor 1 >/dev/null
produce kafka-solo kafka-solo:9092 ex6 300
echo "  messages before the accident: $(count kafka-solo kafka-solo:9092 ex6)"
"${LAB[@]}" rm -sf kafka-solo >/dev/null 2>&1
docker volume rm "${PROJECT}_kafka-solo-data" >/dev/null
echo "  kafka-solo stopped and its volume DELETED"
"${LAB[@]}" up -d kafka-solo >/dev/null 2>&1
wait_ready kafka-solo kafka-solo:9092
echo "  topics after restarting on an EMPTY disk: [$("${LAB[@]}" exec -T kafka-solo env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-solo:9092 --list 2>/dev/null | tr '\n' ' ')]"
echo "  (nothing to count: the topic, its messages and every consumer offset are gone)"
