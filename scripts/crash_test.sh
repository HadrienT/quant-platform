#!/usr/bin/env bash
# CRASH TEST — the acceptance criterion of the audit sink (blueprint WP 02).
#
# Produce N events, kill -9 the sink in the MIDDLE of the flow, restart it, wait
# for the end, count: exactly N rows, no duplicate.
#
# To make the crash land where it hurts, the sink runs with small batches and a
# pause AFTER the database commit and BEFORE the Kafka offset commit
# (SINK_DEBUG_DELAY_MS): a kill there forces the batch to be re-read, and only the
# idempotent insert stands between that and a duplicate.
#
#   scripts/crash_test.sh [N]        default N=1000
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

N="${1:-1000}"
TAG="crash-$(openssl rand -hex 3)"

cleanup() { docker compose up -d --no-deps audit-sink >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "→ producing $N events (username=$TAG) while the sink is stopped, so the backlog is real"
docker compose stop audit-sink >/dev/null
produce_events "$N" "$TAG"

echo "→ starting the sink with small batches and a pause between DB commit and offset commit"
SINK_BATCH_SIZE=20 SINK_DEBUG_DELAY_MS=150 docker compose up -d --no-deps audit-sink >/dev/null

echo "→ waiting for the flow to be under way, then kill -9"
count=0
for _ in $(seq 1 300); do
  count="$(rows_for "$TAG" || echo 0)"
  ((count > 0)) && break
  sleep 0.2
done
docker compose kill -s KILL audit-sink >/dev/null
at_kill="$(rows_for "$TAG")"
echo "  killed with $at_kill / $N rows in the database"
if ((at_kill == 0 || at_kill >= N)); then
  echo "✗ the kill did not land mid-flow ($at_kill of $N) — rerun, the test proves nothing" >&2
  exit 1
fi

echo "→ restarting the sink"
SINK_BATCH_SIZE=20 SINK_DEBUG_DELAY_MS=0 docker compose up -d --no-deps audit-sink >/dev/null
wait_for_rows "$TAG" "$N" 180

total="$(rows_for "$TAG")"
distinct="$(distinct_for "$TAG")"
redelivered="$(docker compose logs audit-sink 2>/dev/null | grep -o '"duplicates": [0-9]*' | awk '{s += $2} END {print s + 0}')"
echo "  rows=$total distinct=$distinct (duplicates skipped by the idempotent insert since the last (re)creation: $redelivered)"

if [[ "$total" == "$N" && "$distinct" == "$N" ]]; then
  echo "✓ crash test passed: exactly $N rows, no duplicate, no loss"
else
  echo "✗ crash test FAILED: expected $N rows, got $total ($distinct distinct)" >&2
  exit 1
fi
