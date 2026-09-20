#!/usr/bin/env bash
# Create .env from .env.placeholder, filling every secret with fresh random
# values. Refuses to overwrite an existing .env. Run once per checkout folder.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -e .env ]]; then
  echo "✗ .env already exists — not overwriting (delete it yourself if you mean it)" >&2
  exit 1
fi

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

umask 077
cp .env.placeholder .env
while IFS= read -r var; do
  case "$var" in
    KAFKA_CLUSTER_ID) value="$(cluster_id)" ;;
    *_PASSWORD | *_SECRET) value="$(secret)" ;;
    *) continue ;;
  esac
  # Only fill variables that are still empty in the placeholder.
  sed -i -E "s|^(${var}=)[[:space:]]*(#.*)?\$|\\1${value}|" .env
done < <(grep -oE '^[A-Z_]+=' .env.placeholder | tr -d '=')

echo "✓ .env created (mode 600). Keep a copy outside the repository."
