#!/usr/bin/env bash
# ntfy end-to-end test (ADR-017): Grafana's provisioned contact point `quant-platform`
# sends a test alert to ntfy; the script then reads the topic back and checks that
# ntfy rendered it with its `grafana` template (🚨 title, priority 4, our short body).
#
#   scripts/ntfy_e2e.sh               through the configured NTFY_URL: the push reaches
#                                     every device subscribed to the topic. Run it once
#                                     after setting NTFY_URL, to see it arrive.
#   scripts/ntfy_e2e.sh --throwaway   through a random ntfy.sh topic nobody follows
#                                     (the default when NTFY_URL is empty, e.g. in dev)
#
# Alert-rule evaluation and the firing/resolved cycle are covered by alert_e2e.sh;
# this script covers the leg alert_e2e.sh replaces with a local receiver.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_prod.sh
source scripts/lib_prod.sh
require_prod_ack "recreates Grafana and pushes a test notification to the real ntfy topic"
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

GRAFANA="http://127.0.0.1:${QP_GRAFANA_PORT:-3100}"
GRAFANA_PW="$(grep -E '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2-)"
NTFY_URL="$(grep -E '^NTFY_URL=' .env | cut -d= -f2- || true)"
SUMMARY="ntfy end-to-end test $(openssl rand -hex 3)"

throwaway=false
if [[ "${1:-}" == "--throwaway" || -z "$NTFY_URL" ]]; then
  throwaway=true
fi

if $throwaway; then
  NTFY_URL="https://ntfy.sh/qp-e2e-$(openssl rand -hex 16)"
  restore() { docker compose up -d --no-deps --wait grafana >/dev/null 2>&1 || true; }
  trap restore EXIT
  echo "→ pointing Grafana at a throw-away ntfy.sh topic"
  NTFY_URL="$NTFY_URL" docker compose up -d --no-deps --wait grafana >/dev/null
else
  echo "→ using the configured NTFY_URL: subscribed devices will get a test push"
fi

echo "1. Grafana sends a test alert through the provisioned contact point"
contact_point="$(curl -sf -u "admin:$GRAFANA_PW" \
  "$GRAFANA/api/v1/provisioning/contact-points?name=quant-platform")"
# Reuse the provisioned settings as they are (URL and message template), so the test
# exercises exactly what a real alert would.
body="$(python3 -c '
import json, sys
cp = json.loads(sys.argv[1])[0]
print(json.dumps({
    "alert": {
        "labels": {"alertname": "ntfy end-to-end test"},
        "annotations": {"summary": sys.argv[2],
                        "description": "Sent by scripts/ntfy_e2e.sh; no action needed."},
    },
    "receivers": [{"name": cp["name"], "grafana_managed_receiver_configs": [{
        "uid": cp["uid"], "name": cp["name"], "type": cp["type"],
        "settings": cp["settings"], "disableResolveMessage": False}]}],
}))' "$contact_point" "$SUMMARY")"
status="$(curl -sf -u "admin:$GRAFANA_PW" -H 'Content-Type: application/json' -X POST \
  "$GRAFANA/api/alertmanager/grafana/config/api/v1/receivers/test" -d "$body" |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["receivers"][0]["grafana_managed_receiver_configs"][0]["status"])')"
if [[ "$status" == "ok" ]]; then ok "Grafana delivered the webhook (status ok)"; else ko "Grafana delivery status: $status"; fi

echo "2. ntfy rendered it with the grafana template"
# The topic URL is the secret: never print it.
message=""
for _ in $(seq 1 15); do
  message="$(curl -sf "$NTFY_URL/json?poll=1&since=5m" | grep -F "$SUMMARY" | tail -1 || true)"
  [[ -n "$message" ]] && break
  sleep 2
done
if [[ -n "$message" ]]; then
  ok "the message is on the topic"
  check() { python3 -c "import json, sys; m = json.loads(sys.argv[1]); sys.exit(0 if $1 else 1)" "$message"; }
  if check 'm.get("title", "").startswith("🚨 [FIRING:1] ntfy end-to-end test")'; then ok "title rendered by the grafana template"; else ko "unexpected title"; fi
  if check 'm.get("priority") == 4 and "rotating_light" in m.get("tags", [])'; then ok "priority 4 and tag rotating_light"; else ko "priority or tags missing"; fi
  if check '"localhost" not in m["message"] and "no action needed" in m["message"]'; then ok "short body: summary and description only"; else ko "body is not the short template"; fi
else
  ko "nothing arrived on the topic within 30 s"
fi

finish
