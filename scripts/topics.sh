#!/usr/bin/env bash
# Align the broker with topics/topics.yml. Idempotent: run twice, the second run
# changes nothing (it prints `changed=0`).
#
# Runs INSIDE the Kafka image (compose service `topics-init`, on every `up`):
#   docker compose run --rm topics-init
#
#   - creates missing topics (--create --if-not-exists);
#   - raises a partition count when the file asks for more;
#   - REFUSES to reduce a partition count — Kafka cannot — and exits non-zero;
#   - aligns retention.ms and cleanup.policy, only when they differ;
#   - lists topics the file does not know about (never deletes anything).
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
TOPICS_FILE="${TOPICS_FILE:-topics/topics.yml}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}" # single broker (ADR-008)

topics_cmd() { "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "$@"; }
configs_cmd() { "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" "$@"; }

# topics.yml keeps one flat mapping per line; turn each into
# "name partitions retention_ms cleanup_policy".
desired="$(awk '
  /^[[:space:]]*- \{/ {
    line = $0
    sub(/^[[:space:]]*- \{[[:space:]]*/, "", line)
    sub(/[[:space:]]*\}[[:space:]]*(#.*)?$/, "", line)
    n = split(line, parts, /,[[:space:]]*/)
    delete kv
    for (i = 1; i <= n; i++) {
      split(parts[i], p, /:[[:space:]]*/)
      kv[p[1]] = p[2]
    }
    print kv["name"], kv["partitions"], kv["retention_ms"], kv["cleanup_policy"]
  }' "$TOPICS_FILE")"

if [[ -z "$desired" ]]; then
  echo "✗ no topic found in $TOPICS_FILE" >&2
  exit 1
fi
while read -r name partitions retention policy; do
  if [[ -z "$name" || ! "$partitions" =~ ^[0-9]+$ || ! "$retention" =~ ^-?[0-9]+$ || -z "$policy" ]]; then
    echo "✗ malformed entry in $TOPICS_FILE: '$name $partitions $retention $policy'" >&2
    exit 1
  fi
done <<<"$desired"

# Actual state, read once: partitions per topic and dynamic configs per topic.
declare -A actual_partitions actual_retention actual_policy
while read -r name count; do
  actual_partitions["$name"]="$count"
done < <(topics_cmd --describe 2>/dev/null | sed -nE 's/^Topic: ([^[:space:]]+).*PartitionCount: ([0-9]+).*/\1 \2/p')

current=""
while IFS= read -r line; do
  if [[ "$line" =~ ^Dynamic\ configs\ for\ topic\ ([^[:space:]]+)\ are: ]]; then
    current="${BASH_REMATCH[1]}"
  elif [[ -n "$current" && "$line" =~ ^[[:space:]]+retention\.ms=([^[:space:]]+) ]]; then
    actual_retention["$current"]="${BASH_REMATCH[1]}"
  elif [[ -n "$current" && "$line" =~ ^[[:space:]]+cleanup\.policy=([^[:space:]]+) ]]; then
    actual_policy["$current"]="${BASH_REMATCH[1]}"
  fi
done < <(configs_cmd --describe --entity-type topics 2>/dev/null)

changed=0
failed=0
declare -A wanted

while read -r name partitions retention policy; do
  wanted["$name"]=1
  have="${actual_partitions[$name]:-}"

  if [[ -z "$have" ]]; then
    topics_cmd --create --if-not-exists --topic "$name" \
      --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" \
      --config "retention.ms=$retention" --config "cleanup.policy=$policy" >/dev/null
    echo "＋ created  $name (partitions=$partitions retention.ms=$retention cleanup.policy=$policy)"
    changed=$((changed + 1))
    continue
  fi

  if ((partitions < have)); then
    echo "✗ REFUSED  $name: file asks for $partitions partitions, broker has $have — Kafka cannot reduce a partition count (create a ${name%.v*}.v<N+1> topic instead)" >&2
    failed=1
  elif ((partitions > have)); then
    topics_cmd --alter --topic "$name" --partitions "$partitions" >/dev/null
    echo "↑ altered  $name: partitions $have → $partitions"
    changed=$((changed + 1))
  fi

  if [[ "${actual_retention[$name]:-}" != "$retention" || "${actual_policy[$name]:-}" != "$policy" ]]; then
    configs_cmd --alter --entity-type topics --entity-name "$name" \
      --add-config "retention.ms=$retention,cleanup.policy=$policy" >/dev/null
    echo "↻ altered  $name: retention.ms=$retention cleanup.policy=$policy"
    changed=$((changed + 1))
  fi
done <<<"$desired"

for name in "${!actual_partitions[@]}"; do
  if [[ -z "${wanted[$name]:-}" && "$name" != __* ]]; then
    echo "⚠ unmanaged topic on the broker (not in $TOPICS_FILE, left untouched): $name"
  fi
done

echo "topics: changed=$changed"
exit "$failed"
