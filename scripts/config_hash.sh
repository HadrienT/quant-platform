#!/usr/bin/env bash
# Write, into .env, one hash per service of the config files it mounts.
#
# Why: `docker compose up -d` recreates a container when ITS DEFINITION changes,
# not when a file bind-mounted into it changes. Without this, editing
# prometheus/prometheus.yml (or adding a migration, or a topic) and redeploying would
# silently keep running the old configuration. The compose file puts each hash in a
# container label, so a config change becomes a definition change: the service is
# recreated — and an unchanged config recreates nothing. (The one-shot jobs
# topics-init and audit-migrate need no hash: compose re-runs them on every `up`.)
#
# deploy.sh runs this; in development run it (or scripts/up.sh) after editing a config.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || {
  echo "✗ no .env — run scripts/init_env.sh" >&2
  exit 1
}

# hash_of PATH… → 12 hex chars over the names and contents of every file below them
hash_of() {
  find "$@" -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -c1-12
}

set_var() {
  if grep -qE "^$1=" .env; then
    sed -i -E "s|^$1=.*|$1=$2|" .env
  else
    printf '%s=%s\n' "$1" "$2" >>.env
  fi
}

set_var QP_HASH_PROMETHEUS "$(hash_of prometheus)"
set_var QP_HASH_LOKI "$(hash_of loki)"
set_var QP_HASH_TEMPO "$(hash_of tempo)"
set_var QP_HASH_OTEL "$(hash_of otel)"
set_var QP_HASH_GRAFANA "$(hash_of grafana alerts)"
set_var QP_HASH_AKHQ "$(hash_of akhq)"
