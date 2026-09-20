#!/usr/bin/env bash
# Set the registry's GLOBAL compatibility mode to BACKWARD (idempotent). Runs as the
# one-shot compose service `registry-init`, inside the Apicurio image (it has curl).
#
# BACKWARD = a consumer using the NEW schema can read data written with the OLD one.
# In practice: you may add a field only with a default, and remove a field freely.
# The registry REFUSES a schema that breaks this (HTTP 409) — that refusal is what
# makes a producer's CI fail instead of a consumer failing in production.
set -euo pipefail

REGISTRY_URL="${REGISTRY_URL:-http://schema-registry:8080}"
API="$REGISTRY_URL/apis/ccompat/v7"

curl -sf -X PUT "$API/config" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"BACKWARD"}' >/dev/null

mode="$(curl -sf "$API/config" | sed -E 's/.*"compatibilityLevel":"([A-Z_]+)".*/\1/')"
if [[ "$mode" != "BACKWARD" ]]; then
  echo "✗ global compatibility is '$mode', expected BACKWARD" >&2
  exit 1
fi
echo "registry: global compatibility = $mode"
