"""Run every query of every provisioned dashboard through Grafana and fail on errors.

A dashboard can be valid JSON and still hold a broken query. Empty results are fine
(the stack may be idle); a query that ERRORS is not.

    python3 scripts/check_dashboards.py          # needs the stack up and .env
"""

import base64
import json
import pathlib
import sys
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent


def env(name: str) -> str:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} missing from .env")


def main() -> int:
    port = (
        env("QP_GRAFANA_PORT")
        if "QP_GRAFANA_PORT=" in (ROOT / ".env").read_text()
        else "3100"
    )
    base = f"http://127.0.0.1:{port}"
    auth = base64.b64encode(f"admin:{env('GRAFANA_ADMIN_PASSWORD')}".encode()).decode()

    def post(path: str, body: dict) -> dict:
        req = urllib.request.Request(
            base + path,
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)

    failures = checked = 0
    for path in sorted((ROOT / "grafana" / "dashboards").glob("*.json")):
        dash = json.loads(path.read_text())
        for panel in dash["panels"]:
            for target in panel.get("targets", []):
                query = dict(target)
                # Dashboard variables: use a plausible value so the query is well-formed.
                for var in dash["templating"]["list"]:
                    for key in ("expr", "rawSql"):
                        if key in query:
                            query[key] = query[key].replace(
                                f"${var['name']}", var["current"]["value"]
                            )
                query["intervalMs"] = 15000
                query["maxDataPoints"] = 500
                checked += 1
                try:
                    result = post(
                        "/api/ds/query",
                        {"queries": [query], "from": "now-6h", "to": "now"},
                    )
                    error = next(
                        (
                            r["error"]
                            for r in result["results"].values()
                            if r.get("error")
                        ),
                        None,
                    )
                except (
                    Exception
                ) as exc:  # noqa: BLE001 — report any transport/HTTP error
                    error = str(exc)
                if error:
                    failures += 1
                    print(
                        f"✗ {dash['uid']} / {panel['title']}: {error}", file=sys.stderr
                    )
    print(f"{checked - failures}/{checked} dashboard queries ran without error")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
