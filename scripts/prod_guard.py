"""Python twin of scripts/lib_prod.sh (ADR-018, issue #13).

There is one Docker stack, the production one: a script that reads `.env` or talks to the
stack must run from the production worktree, or it uses the wrong secrets (and, through
Compose, could recreate production with them).
"""

import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def prod_dir() -> pathlib.Path:
    default = pathlib.Path.home() / "quant-platform-prod"
    return pathlib.Path(os.environ.get("QP_PROD_DIR") or default).resolve()


def require_prod_worktree(script: str) -> None:
    if ROOT != prod_dir():
        sys.exit(
            f"✗ {script} drives the production stack, and this checkout ({ROOT}) is not\n"
            f"  the production worktree ({prod_dir()}). There is no development stack\n"
            f"  (ADR-018). Run it from {prod_dir()} instead."
        )


def require_prod_ack(script: str, effect: str) -> None:
    require_prod_worktree(script)
    if os.environ.get("QP_PROD_TESTS") != "1":
        sys.exit(
            f"✗ {script} runs against PRODUCTION and {effect}.\n"
            f"  Re-run with QP_PROD_TESTS=1 to confirm."
        )
    print(f"⚠ running against production: {effect}")
