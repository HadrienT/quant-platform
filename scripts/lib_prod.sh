# shellcheck shell=bash
# Guards for every script that drives the platform's Docker stack (ADR-018, issue #13).
#
# There is ONE stack: docker-compose.yml pins the project name (`quant-platform`) and the
# container names, so Compose run from ANY checkout drives the production containers and
# volumes, with that checkout's `.env`. From the development worktree this recreated prod
# with dev secrets on 2026-11-23 (Kafka down: cluster id mismatch). Hence:
#
#   require_prod_worktree            refuse unless this checkout IS the production worktree
#   require_prod_ack "what it does"  also refuse unless QP_PROD_TESTS=1: for tests that stop
#                                    or recreate prod services, or write test events into
#                                    the append-only audit trail (they stay there forever)
#
# QP_PROD_DIR overrides the production worktree path (default ~/quant-platform-prod).
# Source it from the repository root (the scripts `cd` there first).

require_prod_worktree() {
  local here prod
  here="$(cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)" && pwd -P)"
  prod="$(cd "${QP_PROD_DIR:-$HOME/quant-platform-prod}" 2>/dev/null && pwd -P || true)"
  if [[ -z "$prod" || "$here" != "$prod" ]]; then
    cat >&2 <<EOF
✗ $(basename "$0") drives the production stack, and this checkout ($here) is not the
  production worktree (${QP_PROD_DIR:-$HOME/quant-platform-prod}).
  There is no development stack (ADR-018): Compose from here would recreate PRODUCTION
  with this checkout's .env. Run it from the production worktree instead:
      ${QP_PROD_DIR:-$HOME/quant-platform-prod}/scripts/$(basename "$0")
  Anything destructive belongs in the disposable lab (scripts/lab_up.sh).
EOF
    exit 2
  fi
}

require_prod_ack() {
  require_prod_worktree
  if [[ "${QP_PROD_TESTS:-}" != "1" ]]; then
    cat >&2 <<EOF
✗ $(basename "$0") runs against PRODUCTION and $1.
  Re-run with QP_PROD_TESTS=1 to confirm:
      QP_PROD_TESTS=1 $0
EOF
    exit 2
  fi
  echo "⚠ running against production: $1"
}
