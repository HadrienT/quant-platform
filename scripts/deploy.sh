#!/usr/bin/env bash
# (Re)start the platform from the production worktree.
#
#   ~/quant-platform-prod/scripts/deploy.sh
#
# Idempotent, and safe on boot (the systemd unit calls it): a second run with
# nothing new changes nothing. Only meant to run from ~/quant-platform-prod.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "✗ .env is missing. Run scripts/init_env.sh (see docs/RUNBOOK.md §2)." >&2
  exit 1
fi

# A newer lot may need variables this .env does not have yet: add them (never
# touching existing values), so the deploy does not fail on a missing secret.
./scripts/init_env.sh --sync

# Fast-forward to the remote when there is one and it is reachable. Non-fatal:
# at boot the network may not be up yet, and a stale checkout still deploys a
# working (older) platform.
if git rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then
  git pull --ff-only --quiet 2>/dev/null \
    && echo "→ synced with $(git rev-parse --abbrev-ref '@{u}')" \
    || echo "⚠ git pull skipped (offline or diverged) — deploying the current checkout"
else
  echo "→ no upstream configured — deploying the current checkout"
fi

docker network inspect dataplatform >/dev/null 2>&1 || docker network create dataplatform

COMMIT_SHA="$(git rev-parse --short HEAD)"
export COMMIT_SHA
echo "→ deploying $COMMIT_SHA"

if [[ -z "$(docker compose config --services)" ]]; then
  echo "✓ no service defined yet (WP 00) — nothing to start"
  exit 0
fi

# Persist the tag so a bare `docker compose up -d` (systemd unit, manual call)
# runs THIS build, not whatever COMMIT_SHA last resolved to.
if grep -qE '^COMMIT_SHA=' .env; then
  sed -i "s/^COMMIT_SHA=.*/COMMIT_SHA=${COMMIT_SHA}/" .env
else
  printf '\nCOMMIT_SHA=%s\n' "${COMMIT_SHA}" >>.env
fi

docker compose build
# --wait: return only once every service with a healthcheck is healthy and every
# one-shot job (topic creation, migrations) has completed successfully.
if ! docker compose up -d --remove-orphans --wait --wait-timeout 300; then
  echo "✗ platform did not become healthy — last logs:" >&2
  docker compose ps -a >&2
  docker compose logs --tail 30 >&2
  exit 1
fi

docker image prune -f >/dev/null
docker compose ps
echo "✓ deployed $COMMIT_SHA"
