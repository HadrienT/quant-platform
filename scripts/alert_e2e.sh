#!/usr/bin/env bash
# End-to-end alert test (blueprint WP 04): a fallback event goes through Kafka →
# data-quality → Prometheus → the Grafana rule "Live fallback", which must go
# FIRING, POST a notification, then return to NORMAL and POST a "resolved" one.
#
# The notification is caught by a throw-away local webhook receiver, so this proves
# Grafana's delivery whatever real channel (ntfy, e-mail bridge…) ALERT_WEBHOOK_URL
# points at in production. Takes about 6-8 minutes: the rule looks back 3 minutes.
#
#   scripts/alert_e2e.sh
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

GRAFANA="http://127.0.0.1:${QP_GRAFANA_PORT:-3100}"
GRAFANA_PW="$(grep -E '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2-)"
PROBE="qp-alert-probe"
NETWORK="quant-platform_default"
RULE="Live fallback"

cleanup() {
  docker rm -f "$PROBE" >/dev/null 2>&1 || true
  # Back to the configured channel (or the "not configured" default).
  docker compose up -d --no-deps grafana >/dev/null 2>&1 || true
}
trap cleanup EXIT

rule_state() {
  curl -sf -u "admin:$GRAFANA_PW" "$GRAFANA/api/prometheus/grafana/api/v1/rules" | python3 -c '
import sys, json
for group in json.load(sys.stdin)["data"]["groups"]:
    for rule in group["rules"]:
        if rule["name"] == sys.argv[1]:
            print(rule["state"])
' "$RULE"
}
wait_state() { # STATE TIMEOUT_S
  local i
  for ((i = 0; i < $2; i++)); do
    [[ "$(rule_state 2>/dev/null)" == "$1" ]] && return 0
    sleep 1
  done
  return 1
}
hits() { docker exec "$PROBE" cat /tmp/hits.jsonl 2>/dev/null || true; }
wait_hit() { # PATTERN TIMEOUT_S
  local i
  for ((i = 0; i < $2; i++)); do
    grep -q "$1" <<<"$(hits)" && return 0
    sleep 2
  done
  return 1
}

echo "→ starting a local webhook receiver and pointing Grafana's contact point at it"
docker rm -f "$PROBE" >/dev/null 2>&1 || true
docker run -d --name "$PROBE" --network "$NETWORK" python:3.13.5-slim python -u -c '
import http.server
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        open("/tmp/hits.jsonl", "ab").write(body.replace(b"\n", b" ") + b"\n")
        self.send_response(200); self.end_headers()
    def log_message(self, *a): pass
http.server.HTTPServer(("0.0.0.0", 8080), H).serve_forever()
' >/dev/null
ALERT_WEBHOOK_URL="http://$PROBE:8080/" docker compose up -d --no-deps --wait grafana >/dev/null

echo "1. Baseline: the rule is normal"
if wait_state inactive 120; then ok "rule '$RULE' is Normal before the fallback"; else ko "rule is '$(rule_state)', expected inactive"; fi

echo "2. A fallback event enters the stream"
TAG="alert-$(openssl rand -hex 3)"
python3 scripts/gen_events.py 1 "$TAG" --type data.fallback --payload '{"kind":"stale_data"}' >"$TAG.txt"
produce_file qm.dataquality.fallback.v1 "$TAG.txt"
rm -f "$TAG.txt"
if wait_state firing 240; then ok "rule went FIRING"; else ko "rule never fired (state: $(rule_state))"; fi
if wait_hit '"status":"firing"' 120 && grep -q "$RULE" <<<"$(hits)"; then ok "a 'firing' notification was POSTed to the webhook"; else ko "no firing notification received"; fi

echo "3. The event ages out of the 3-minute window: the alert resolves"
if wait_state inactive 600; then ok "rule returned to Normal"; else ko "rule still '$(rule_state)'"; fi
if wait_hit '"status":"resolved"' 180; then ok "a 'resolved' notification was POSTed"; else ko "no resolved notification received"; fi

finish
