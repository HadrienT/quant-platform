#!/usr/bin/env bash
# Cardinality check (CLAUDE.md principle 8): no Prometheus label and no Loki index
# label may be a ticker, a user, a request_id or an IP. Fails, naming the offender.
#
#   scripts/check_labels.sh                # against the running stack
#   scripts/check_labels.sh --self-test    # proves the matcher itself catches offenders
set -euo pipefail
cd "$(dirname "$0")/.."

PROM_URL="${PROM_URL:-http://127.0.0.1:${QP_PROMETHEUS_PORT:-9091}}"
LOKI_URL="${LOKI_URL:-http://127.0.0.1:${QP_LOKI_PORT:-3101}}"

# Label names that would create one series/stream per ticker, user, request or address.
FORBIDDEN='^(ticker|symbol|username|user|user_id|userid|request_id|trace_id|span_id|event_id|ip|client_ip|remote_addr|ip_hash)$'

# offenders < one label name per line → the forbidden ones
offenders() { grep -E "$FORBIDDEN" || true; }

if [[ "${1:-}" == "--self-test" ]]; then
  bad="$(printf '%s\n' job instance ticker request_id le ip user | offenders | tr '\n' ' ')"
  good="$(printf '%s\n' job instance le product engine model outcome kind | offenders | tr '\n' ' ')"
  [[ "$bad" == "ticker request_id ip user " ]] || {
    echo "✗ self-test: matcher missed offenders (got: '$bad')" >&2
    exit 1
  }
  [[ -z "$good" ]] || {
    echo "✗ self-test: matcher flagged legitimate labels ('$good')" >&2
    exit 1
  }
  echo "✓ self-test: the matcher catches offenders and lets legitimate labels through"
  exit 0
fi

fail=0

prom_labels="$(curl -sf "$PROM_URL/api/v1/labels" | python3 -c 'import sys,json; print("\n".join(json.load(sys.stdin)["data"]))')" ||
  {
    echo "✗ cannot query Prometheus at $PROM_URL" >&2
    exit 2
  }
found="$(offenders <<<"$prom_labels" | tr '\n' ' ')"
if [[ -n "$found" ]]; then
  echo "✗ Prometheus exposes forbidden label(s): $found" >&2
  fail=1
else
  echo "✓ Prometheus: no forbidden label ($(wc -l <<<"$prom_labels") label names checked)"
fi

loki_labels="$(curl -sf "$LOKI_URL/loki/api/v1/labels" | python3 -c 'import sys,json; print("\n".join(json.load(sys.stdin)["data"]))')" ||
  {
    echo "✗ cannot query Loki at $LOKI_URL" >&2
    exit 2
  }
found="$(offenders <<<"$loki_labels" | tr '\n' ' ')"
if [[ -n "$found" ]]; then
  echo "✗ Loki indexes forbidden label(s): $found" >&2
  fail=1
else
  echo "✓ Loki: no forbidden index label ($(wc -l <<<"$loki_labels") label names checked)"
fi

exit "$fail"
