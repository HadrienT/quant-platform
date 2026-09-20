#!/usr/bin/env bash
# Create .venv with the pinned developer tooling (black, yamllint, shellcheck,
# pytest, pip-tools) from requirements-dev.txt. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -x .venv/bin/python ]]; then
  if command -v uv >/dev/null 2>&1; then
    uv venv --seed --quiet --python 3.13 .venv
  else
    python3 -m venv .venv
  fi
fi
.venv/bin/python -m pip install --quiet --disable-pip-version-check -r requirements-dev.txt
echo "✓ .venv ready — scripts/make.sh will pick it up"
