#!/usr/bin/env bash
# Create .env from .env.placeholder, filling every secret with fresh random values.
#
#   scripts/init_env.sh          create .env (refuses to overwrite an existing one)
#   scripts/init_env.sh --sync   append the variables a newer .env.placeholder has and
#                                .env lacks (new lot = new variables). Never modifies
#                                or removes an existing value. deploy.sh calls this.
set -euo pipefail
cd "$(dirname "$0")/.."

mode="${1:-create}"

secret() { openssl rand -hex 24; }

# Kafka cluster id: 16 random bytes, url-safe base64 without padding (22 chars).
# Kafka rejects ids that start with '-'.
cluster_id() {
  local id
  while :; do
    id="$(head -c 16 /dev/urandom | base64 | tr '+/' '-_' | cut -c1-22)"
    [[ "$id" != -* ]] && break
  done
  printf '%s' "$id"
}

# Value for a variable that is empty in the placeholder; empty when it is meant
# to stay empty (optional settings such as ALERT_WEBHOOK_URL).
generated_value() {
  case "$1" in
    KAFKA_CLUSTER_ID) cluster_id ;;
    *_PASSWORD | *_SECRET) secret ;;
    *) printf '' ;;
  esac
}

case "$mode" in
  create)
    if [[ -e .env ]]; then
      echo "✗ .env already exists — not overwriting (delete it yourself if you mean it, or use --sync)" >&2
      exit 1
    fi
    umask 077
    cp .env.placeholder .env
    while IFS= read -r var; do
      value="$(generated_value "$var")"
      [[ -n "$value" ]] || continue
      sed -i -E "s|^(${var}=)[[:space:]]*(#.*)?\$|\\1${value}|" .env
    done < <(grep -oE '^[A-Z_]+=' .env.placeholder | tr -d '=')
    echo "✓ .env created (mode 600). Keep a copy outside the repository."
    ;;
  --sync)
    [[ -f .env ]] || {
      echo "✗ no .env to sync — run scripts/init_env.sh first" >&2
      exit 1
    }
    added=0
    while IFS= read -r line; do
      var="${line%%=*}"
      grep -qE "^${var}=" .env && continue
      value="$(generated_value "$var")"
      if [[ -n "$value" ]]; then
        printf '%s=%s\n' "$var" "$value" >>.env
      else
        # Take the placeholder's own default (may be empty), without its inline comment.
        rest="${line#*=}"
        printf '%s=%s\n' "$var" "$(sed -E 's/[[:space:]]+#.*$//; s/^[[:space:]]+//' <<<"$rest")" >>.env
      fi
      echo "＋ .env: added $var"
      added=$((added + 1))
    done < <(grep -E '^[A-Z_]+=' .env.placeholder)
    echo "env: added=$added"
    ;;
  *)
    echo "usage: $0 [--sync]" >&2
    exit 2
    ;;
esac
