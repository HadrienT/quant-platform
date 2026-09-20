#!/usr/bin/env bash
# Destroy the operator lab completely: containers, volumes and network. Leaves
# nothing behind (checked), and never touches the platform (a different project).
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose -f docker-compose.lab.yml --profile '*' down -v --remove-orphans

leftovers="$(
  {
    docker ps -a --filter 'label=com.docker.compose.project=quant-platform-lab' --format 'container {{.Names}}'
    docker volume ls --filter 'label=com.docker.compose.project=quant-platform-lab' --format 'volume {{.Name}}'
    docker network ls --filter 'label=com.docker.compose.project=quant-platform-lab' --format 'network {{.Name}}'
  }
)"
if [[ -n "$leftovers" ]]; then
  echo "✗ the lab left resources behind:" >&2
  echo "$leftovers" >&2
  exit 1
fi
echo "✓ lab destroyed: no container, volume or network left"
