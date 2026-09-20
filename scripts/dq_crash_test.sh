#!/usr/bin/env bash
# CRASH TEST of the data-quality consumer (blueprint WP 04): kill -9 it in the
# middle of a flow, restart, and check that NO EVENT WAS LOST.
#
# Its aggregates are in-memory counters, so the guarantee is at-least-once: a batch
# re-read after the crash may be processed twice (harmless for a monitoring signal).
# The test therefore asserts "every produced event was processed at least once, and
# the group's lag reached zero", and REPORTS how many were re-read.
#
#   scripts/dq_crash_test.sh [N]        default N=1000
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

N="${1:-1000}"
TAG="dqcrash-$(openssl rand -hex 3)"
FILE="$(mktemp)"
trap 'rm -f "$FILE"; docker compose up -d --no-deps data-quality >/dev/null 2>&1 || true' EXIT

python3 scripts/gen_events.py "$N" "$TAG" --type data.fallback --payload '{"kind":"default_rate"}' >"$FILE"
python3 - "$FILE" >"$FILE.ids" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    print(json.loads(line.split("|", 1)[1])["event_id"])
PY
trap 'rm -f "$FILE" "$FILE.ids"; docker compose up -d --no-deps data-quality >/dev/null 2>&1 || true' EXIT

processed() { docker compose logs --no-log-prefix data-quality 2>/dev/null | grep -c '"msg": "processed"' || true; }

echo "→ producing $N fallback events while data-quality is stopped"
docker compose stop data-quality >/dev/null
produce_file qm.dataquality.fallback.v1 "$FILE"

echo "→ starting it with small batches, a pause before each commit, and per-event logging"
DQ_BATCH_SIZE=20 DQ_DEBUG_DELAY_MS=150 DQ_LOG_EVENTS=1 docker compose up -d --no-deps data-quality >/dev/null
base="$(processed)"
for _ in $(seq 1 300); do
  (($(processed) - base > 0)) && break
  sleep 0.2
done
docker compose kill -s KILL data-quality >/dev/null
at_kill=$(($(processed) - base))
echo "  killed after ~$at_kill of $N events"
if ((at_kill <= 0 || at_kill >= N)); then
  echo "✗ the kill did not land mid-flow — rerun, the test proves nothing" >&2
  exit 1
fi

echo "→ restarting (same container, same logs)"
docker compose start data-quality >/dev/null
for _ in $(seq 1 180); do
  [[ "$(group_lag data-quality qm.dataquality.fallback.v1)" == "0" ]] && break
  sleep 1
done
lag="$(group_lag data-quality qm.dataquality.fallback.v1)"

docker compose logs --no-log-prefix data-quality 2>/dev/null | python3 -c '
import json, sys
wanted = set(open(sys.argv[1]).read().split())
seen = [json.loads(l).get("event_id") for l in sys.stdin if l.startswith("{") and "\"processed\"" in l]
seen = [e for e in seen if e in wanted]
print(len(set(seen)), len(seen) - len(set(seen)))
' "$FILE.ids" >"$FILE.out"
read -r distinct redelivered <"$FILE.out"
rm -f "$FILE.out"
echo "  processed at least once: $distinct / $N   re-read after the crash: $redelivered   group lag: $lag"

if [[ "$distinct" == "$N" && "$lag" == "0" ]]; then
  echo "✓ data-quality crash test passed: no event lost, lag back to 0"
else
  echo "✗ data-quality crash test FAILED" >&2
  exit 1
fi
