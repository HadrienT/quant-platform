#!/usr/bin/env bash
# Independence of the two consumer groups (blueprint WP 04): stopping data-quality
# does not affect the audit sink or its lag, and stopping the sink does not affect
# data-quality. Each group has its own offsets on the same topic.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

trap 'docker compose up -d --no-deps audit-sink data-quality >/dev/null 2>&1 || true' EXIT
TOPIC="$TOPIC_VALUATION"

wait_lag_zero() { # GROUP
  local i
  for ((i = 0; i < 90; i++)); do
    [[ "$(group_lag "$1" "$TOPIC")" == "0" ]] && return 0
    sleep 1
  done
  return 1
}
wait_lag_at_least() { # GROUP N
  local i
  for ((i = 0; i < 60; i++)); do
    (($(group_lag "$1" "$TOPIC") >= $2)) && return 0
    sleep 1
  done
  return 1
}

docker compose up -d --no-deps audit-sink data-quality >/dev/null
if ! { wait_lag_zero audit-sink && wait_lag_zero data-quality; }; then
  ko "groups were not caught up at the start"
  finish
fi

echo "1. Stop data-quality: the sink and its lag are unaffected"
TAG="indep1-$(openssl rand -hex 3)"
docker compose stop data-quality >/dev/null
produce_events 200 "$TAG"
if wait_for_rows "$TAG" 200 60; then ok "the sink archived all 200 events"; else ko "the sink was affected"; fi
if wait_lag_zero audit-sink; then ok "audit-sink lag is 0"; else ko "audit-sink lag is $(group_lag audit-sink "$TOPIC")"; fi
if wait_lag_at_least data-quality 200; then ok "data-quality's own lag grew to $(group_lag data-quality "$TOPIC") (independent offsets)"; else ko "data-quality lag did not grow"; fi
docker compose up -d --no-deps data-quality >/dev/null
if wait_lag_zero data-quality; then ok "data-quality caught up on restart"; else ko "data-quality did not catch up"; fi

echo "2. Stop the sink: data-quality is unaffected"
TAG="indep2-$(openssl rand -hex 3)"
docker compose stop audit-sink >/dev/null
produce_events 200 "$TAG"
if wait_lag_zero data-quality; then ok "data-quality consumed all 200 events (lag 0)"; else ko "data-quality was affected"; fi
if wait_lag_at_least audit-sink 200; then ok "the stopped sink's lag is $(group_lag audit-sink "$TOPIC")"; else ko "sink lag did not grow"; fi
if [[ "$(rows_for "$TAG")" == "0" ]]; then ok "nothing archived while the sink is stopped"; else ko "rows appeared without the sink"; fi
docker compose up -d --no-deps audit-sink >/dev/null
if wait_for_rows "$TAG" 200 90; then ok "the sink archived the backlog on restart"; else ko "backlog not archived"; fi

finish
