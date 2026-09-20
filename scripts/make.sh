#!/usr/bin/env bash
# Single entry point for quality checks — used identically by developers and CI.
#
#   scripts/make.sh
#
# Validates the infrastructure BEFORE anything is started: compose file, the
# non-negotiable Docker rules (scripts/check_compose.py), YAML, shell, Python
# formatting, then the unit tests. Exits non-zero on the first failing step.
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -d .venv/bin ]]; then
  PATH="$PWD/.venv/bin:$PATH"
fi

for tool in docker yamllint shellcheck black python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "✗ $tool not found — run scripts/bootstrap.sh (developer tooling)" >&2
    exit 1
  fi
done

step() { printf '\n── %s\n' "$*"; }

# Compose interpolates ${VAR:?} — validate against the placeholder with every
# empty value filled in, so the check never needs real secrets.
envfile="$(mktemp)"
trap 'rm -f "$envfile"' EXIT
sed -E 's/^([A-Za-z_][A-Za-z0-9_]*)=[[:space:]]*(#.*)?$/\1=placeholder/' .env.placeholder >"$envfile"

step "docker compose config"
docker compose --env-file "$envfile" --profile '*' config -q

step "compose policy (pinned images, limits, loopback-only ports)"
docker compose --env-file "$envfile" --profile '*' config --format json | python3 scripts/check_compose.py

step "lab compose (WP 06): valid, and the same policy"
docker compose -f docker-compose.lab.yml --profile '*' config -q
docker compose -f docker-compose.lab.yml --profile '*' config --format json | python3 scripts/check_compose.py

step "yamllint"
yamllint .

step "shellcheck"
mapfile -t shell_files < <(find . -name '*.sh' -not -path './.venv/*' -not -path './.git/*' | sort)
if ((${#shell_files[@]})); then
  shellcheck "${shell_files[@]}"
fi

step "black --check"
mapfile -t py_files < <(find . -name '*.py' -not -path './.venv/*' -not -path './.git/*' | sort)
if ((${#py_files[@]})); then
  black --check --quiet "${py_files[@]}"
fi

step "pytest"
if ((${#py_files[@]})) && grep -rqE '^\s*def test_' --include='test_*.py' . 2>/dev/null; then
  pytest -q
else
  echo "(no tests yet)"
fi

printf '\n✓ all checks passed\n'
