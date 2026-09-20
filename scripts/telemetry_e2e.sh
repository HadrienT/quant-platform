#!/usr/bin/env bash
# End-to-end telemetry checks against a RUNNING stack (blueprint WP 03):
#   1. an OTLP metric, trace and log sent to the collector reach Prometheus, Tempo
#      and Loki — and a forbidden label (request_id) sent with the metric is dropped;
#   2. the consumer lag of group audit-sink RISES when the sink stops and FALLS back
#      when it restarts.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

PROM="http://127.0.0.1:${QP_PROMETHEUS_PORT:-9091}"
LOKI="http://127.0.0.1:${QP_LOKI_PORT:-3101}"
OTLP="http://127.0.0.1:${QP_OTLP_HTTP_PORT:-4318}"
GRAFANA="http://127.0.0.1:${QP_GRAFANA_PORT:-3100}"
GRAFANA_PW="$(grep -E '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2-)"

trap 'docker compose up -d --no-deps audit-sink >/dev/null 2>&1 || true' EXIT

prom_value() { # PROMQL → first sample value, or empty
  curl -sfG "$PROM/api/v1/query" --data-urlencode "query=$1" |
    python3 -c 'import sys,json; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")'
}
wait_until() { # TIMEOUT_S command… → 0 as soon as the command succeeds
  local timeout="$1" i
  shift
  for ((i = 0; i < timeout; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}

RUN="$(openssl rand -hex 3)"
SERVICE="telemetry-e2e-$RUN"
TRACE_ID="$(openssl rand -hex 16)"
SPAN_ID="$(openssl rand -hex 8)"
NOW_NS="$(date +%s%N)"
RESOURCE="{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"$SERVICE\"}}]}"

echo "1. OTLP → collector → Prometheus / Tempo / Loki"
metric="qm_e2e_probe_${RUN}_total"
curl -sf -X POST "$OTLP/v1/metrics" -H 'Content-Type: application/json' -d "{\"resourceMetrics\":[{\"resource\":$RESOURCE,\"scopeMetrics\":[{\"metrics\":[{\"name\":\"$metric\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[{\"asInt\":\"1\",\"timeUnixNano\":\"$NOW_NS\",\"attributes\":[{\"key\":\"outcome\",\"value\":{\"stringValue\":\"ok\"}},{\"key\":\"request_id\",\"value\":{\"stringValue\":\"req_$RUN\"}},{\"key\":\"ticker\",\"value\":{\"stringValue\":\"SPY\"}}]}]}}]}]}]}" >/dev/null
curl -sf -X POST "$OTLP/v1/traces" -H 'Content-Type: application/json' -d "{\"resourceSpans\":[{\"resource\":$RESOURCE,\"scopeSpans\":[{\"spans\":[{\"traceId\":\"$TRACE_ID\",\"spanId\":\"$SPAN_ID\",\"name\":\"e2e-probe\",\"kind\":1,\"startTimeUnixNano\":\"$NOW_NS\",\"endTimeUnixNano\":\"$((NOW_NS + 5000000))\"}]}]}]}" >/dev/null
curl -sf -X POST "$OTLP/v1/logs" -H 'Content-Type: application/json' -d "{\"resourceLogs\":[{\"resource\":$RESOURCE,\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"$NOW_NS\",\"body\":{\"stringValue\":\"{\\\"msg\\\":\\\"probe\\\",\\\"trace_id\\\":\\\"$TRACE_ID\\\"}\"}}]}]}]}" >/dev/null

metric_present() { [[ -n "$(prom_value "$metric")" ]]; }
if wait_until 60 metric_present; then ok "metric reached Prometheus"; else ko "metric never reached Prometheus"; fi
labels="$(curl -sfG "$PROM/api/v1/series" --data-urlencode "match[]=$metric" | python3 -c 'import sys,json; print(" ".join(sorted(json.load(sys.stdin)["data"][0])))')"
if grep -q 'outcome' <<<"$labels"; then ok "legitimate label kept (outcome)"; else ko "outcome label missing: $labels"; fi
if grep -qE '(^| )(request_id|ticker)( |$)' <<<"$labels"; then ko "forbidden label reached Prometheus: $labels"; else ok "request_id and ticker were dropped by the collector"; fi

tempo_get() { curl -sf -u "admin:$GRAFANA_PW" "$GRAFANA/api/datasources/proxy/uid/tempo/api/traces/$TRACE_ID"; }
if wait_until 60 tempo_get; then ok "trace found in Tempo by trace_id (through Grafana's data source)"; else ko "trace not found in Tempo"; fi

loki_query() { curl -sfG "$LOKI/loki/api/v1/query_range" --data-urlencode "query={service_name=\"$SERVICE\"} |= \"$TRACE_ID\"" | grep -q "$TRACE_ID"; }
if wait_until 60 loki_query; then ok "log found in Loki under label service_name=$SERVICE"; else ko "log not found in Loki"; fi

echo "2. Consumer lag of audit-sink rises when the sink stops and falls when it restarts"
lag() { prom_value 'sum(kafka_consumergroup_lag{consumergroup="audit-sink"})'; }
lag_at_least() { [[ -n "$(lag)" ]] && (($(printf '%.0f' "$(lag)") >= $1)); }
lag_is_zero() { [[ -n "$(lag)" ]] && (($(printf '%.0f' "$(lag)") == 0)); }

TAG="lag-$RUN"
docker compose stop audit-sink >/dev/null
produce_events 300 "$TAG"
if wait_until 120 lag_at_least 300; then ok "lag rose to $(lag) while the sink was stopped"; else ko "lag did not rise (now: $(lag))"; fi
docker compose up -d --no-deps audit-sink >/dev/null
if wait_until 180 lag_is_zero; then ok "lag fell back to 0 after the restart"; else ko "lag did not recover (now: $(lag))"; fi
if wait_for_rows "$TAG" 300 30; then ok "all 300 events archived"; else ko "events missing"; fi

finish
