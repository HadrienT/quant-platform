#!/usr/bin/env bash
# Lab exercise 5: replay at scale. Produce N events once, then for each sink batch size
# empty the table, rewind the group to the start and time how long the sink takes to
# archive all N; sample CPU of each component to see where the bottleneck is.
#
#   scripts/lab_replay.sh [N] [BATCH_SIZE…]      default N=1000000, batch sizes 50 500 5000
# Needs: scripts/lab_up.sh replay   (and the audit-sink image built: scripts/up.sh once)
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-1000000}"
shift || true
SIZES=("$@")
((${#SIZES[@]})) || SIZES=(50 500 5000)

LAB=(docker compose -f docker-compose.lab.yml --profile replay)
TOPIC=qm.audit.valuation.v1
K() {
  local tool="$1"
  shift
  "${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx64m -Xms32m" "/opt/kafka/bin/$tool" --bootstrap-server kafka-1:9092 "$@"
}
psql_admin() { "${LAB[@]}" exec -T lab-audit psql -U qm_admin -d qm_audit -X -At "$@"; }
rows() { psql_admin -c 'SELECT count(*) FROM audit.events' 2>/dev/null || echo 0; }

echo "→ database and topic"
"${LAB[@]}" up -d lab-audit lab-migrate >/dev/null
K kafka-topics.sh --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 3 --config retention.ms=86400000 >/dev/null

have="$(K kafka-get-offsets.sh --topic "$TOPIC" --time -1 | awk -F: '{s += $3} END {print s + 0}')"
if ((have < N)); then
  echo "→ producing $((N - have)) events (unique UUID v7 each; the sink dedups on event_id)"
  start=$SECONDS
  python3 scripts/gen_events.py "$((N - have))" replay | "${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx128m" \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic "$TOPIC" \
    --property parse.key=true --property 'key.separator=|' \
    --producer-property acks=1 --producer-property linger.ms=50 --producer-property batch.size=262144 \
    --producer-property compression.type=lz4 >/dev/null
  echo "  produced in $((SECONDS - start)) s"
fi
echo "  topic holds $(K kafka-get-offsets.sh --topic "$TOPIC" --time -1 | awk -F: '{s += $3} END {print s + 0}') messages"

printf '\n%-10s %-10s %-12s | CPU%% (mean while replaying)\n' "batch" "seconds" "events/s"
printf '%-10s %-10s %-12s | %-10s %-10s %-10s\n' "" "" "" "sink" "postgres" "broker(avg)"
for size in "${SIZES[@]}"; do
  "${LAB[@]}" stop lab-sink >/dev/null 2>&1 || true
  psql_admin -c 'TRUNCATE audit.events' >/dev/null
  K kafka-consumer-groups.sh --group audit-sink --reset-offsets --to-earliest --all-topics --execute >/dev/null 2>&1 || true

  LAB_SINK_BATCH_SIZE="$size" "${LAB[@]}" up -d --force-recreate lab-sink >/dev/null 2>&1
  start=$SECONDS
  samples=""
  while :; do
    now="$(rows)"
    if ((now >= N)); then break; fi
    if ((SECONDS - start > 900)); then
      echo "timeout at $now rows" >&2
      break
    fi
    samples+="$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' 2>/dev/null | grep -E 'quant-platform-lab-(lab-sink|lab-audit|kafka-[123])' | tr -d '%')"$'\n'
    sleep 2
  done
  elapsed=$((SECONDS - start))
  ((elapsed > 0)) || elapsed=1
  cpu="$(awk '
    /lab-sink/  {s += $2; ns++}
    /lab-audit/ {p += $2; np++}
    /kafka-/    {k += $2; nk++}
    END {printf "%-10.0f %-10.0f %-10.0f", (ns ? s / ns : 0), (np ? p / np : 0), (nk ? k / nk : 0)}' <<<"$samples")"
  printf '%-10s %-10s %-12s | %s\n' "$size" "$elapsed" "$((N / elapsed))" "$cpu"
done
echo "(CPU% is per container, 100 = one core; the sink and Postgres are capped at 2 cores, each broker at 1.)"
