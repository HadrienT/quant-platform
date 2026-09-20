#!/usr/bin/env bash
# Start the disposable operator lab (docker-compose.lab.yml, project quant-platform-lab).
#
#   scripts/lab_up.sh [profile…]     e.g.  scripts/lab_up.sh replay      (sink + Postgres)
#                                          scripts/lab_up.sh sasl solo
#
# Safety first: the exercises kill brokers and rewrite offsets. This refuses to start
# unless the lab is a separate project, attached to NO shared network and publishing
# NO port — so it cannot reach (or be mistaken for) the running platform.
set -euo pipefail
cd "$(dirname "$0")/.."

LAB=(docker compose -f docker-compose.lab.yml)

config="$("${LAB[@]}" --profile '*' config --format json)"
python3 - "$config" <<'PY'
import json, sys
cfg = json.loads(sys.argv[1])
problems = []
if cfg["name"] != "quant-platform-lab":
    problems.append(f"project name is {cfg['name']!r}, expected quant-platform-lab")
for name, net in (cfg.get("networks") or {}).items():
    if net.get("external") or name == "dataplatform":
        problems.append(f"network {name!r} is external/shared")
for name, svc in cfg["services"].items():
    if svc.get("ports"):
        problems.append(f"service {name} publishes ports")
    if "dataplatform" in (svc.get("networks") or {}):
        problems.append(f"service {name} is attached to dataplatform")
    if svc.get("container_name"):
        problems.append(f"service {name} sets container_name (could collide with the platform's)")
if problems:
    print("✗ the lab is not isolated:\n  " + "\n  ".join(problems), file=sys.stderr)
    sys.exit(1)
print("✓ lab isolation verified: own project, no shared network, no published port")
PY

profiles=()
for p in "$@"; do profiles+=(--profile "$p"); done

"${LAB[@]}" "${profiles[@]}" up -d kafka-1 kafka-2 kafka-3
for _ in $(seq 1 60); do
  if "${LAB[@]}" exec -T kafka-1 env KAFKA_HEAP_OPTS="-Xmx64m" \
    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-3:9092 >/dev/null 2>&1; then
    echo "✓ 3-broker cluster is up"
    exit 0
  fi
  sleep 2
done
echo "✗ the cluster did not come up" >&2
"${LAB[@]}" logs --tail 20 >&2
exit 1
