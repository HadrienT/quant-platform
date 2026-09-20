"""Health gate for `docker compose up -d` (used by deploy.sh and up.sh).

    python3 scripts/wait_healthy.py [TIMEOUT_S]      default 300

Why not `docker compose up --wait`? It treats a one-shot job that finishes (topics-init,
audit-migrate, registry-init: exit code 0) as a failure unless another service depends on
it, so a healthy platform could fail its own deploy depending on timing. Here the rule is
explicit, from the compose file itself:

  - a service with `restart: "no"` (a one-shot job) must end up `exited` with code 0;
  - every other service must be `running`, and `healthy` when it has a healthcheck.

Exits 0 when all hold; non-zero as soon as something has definitely failed (a job exited
non-zero, a long-running service exited), or when the timeout expires.
"""

import json
import subprocess
import sys
import time


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", *args], capture_output=True, text=True, check=True
    ).stdout


def evaluate(rows: list[dict], oneshot: set[str], expected: set[str]):
    """Return (state, details): state is 'ok', 'pending' or 'failed'."""
    seen = {r["Service"]: r for r in rows}
    pending: list[str] = []
    failed: list[str] = []
    for name in sorted(expected):
        row = seen.get(name)
        if row is None:
            pending.append(f"{name}: not created yet")
            continue
        state, health, code = (
            row["State"],
            row.get("Health") or "",
            row.get("ExitCode", 0),
        )
        if name in oneshot:
            if state == "exited" and code == 0:
                continue
            if state == "exited":
                failed.append(f"{name}: job exited with code {code}")
            else:
                pending.append(f"{name}: job is {state}")
        else:
            if state == "running" and health in ("", "healthy"):
                continue
            if state == "exited" or health == "unhealthy":
                failed.append(f"{name}: {state} {health}".strip())
            else:
                pending.append(f"{name}: {state} {health}".strip())
    if failed:
        return "failed", failed
    if pending:
        return "pending", pending
    return "ok", []


def main() -> int:
    timeout = float(sys.argv[1]) if len(sys.argv) > 1 else 300
    cfg = json.loads(compose("config", "--format", "json"))
    services = cfg["services"]
    oneshot = {n for n, s in services.items() if s.get("restart", "no") == "no"}
    expected = set(services)

    deadline = time.time() + timeout
    while True:
        rows = [
            json.loads(line)
            for line in compose("ps", "-a", "--format", "json").splitlines()
            if line.strip()
        ]
        state, details = evaluate(rows, oneshot, expected)
        if state == "ok":
            print(
                f"✓ {len(expected)} services as expected ({len(oneshot)} one-shot jobs finished, {len(expected) - len(oneshot)} running)"
            )
            return 0
        if state == "failed" or time.time() > deadline:
            print(
                "✗ "
                + ("failed" if state == "failed" else f"timeout after {timeout:.0f}s"),
                file=sys.stderr,
            )
            for line in details:
                print(f"   {line}", file=sys.stderr)
            return 1
        time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
