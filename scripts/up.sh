#!/usr/bin/env bash
# Development: refresh the config hashes, then bring the stack up and wait for it.
# (deploy.sh does the same in production, plus pull/build/health gate.)
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/init_env.sh --sync >/dev/null
./scripts/config_hash.sh
docker compose up -d --build "$@"
# (not `up --wait`: it fails on finished one-shot jobs — see scripts/wait_healthy.py)
if (($# == 0)); then
  python3 scripts/wait_healthy.py 300
fi
