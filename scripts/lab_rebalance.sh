#!/usr/bin/env bash
# Lab exercise 2: three consumers of one group, kill one, measure the pause.
#
#   scripts/lab_rebalance.sh STRATEGY MODE
#     STRATEGY  range | cooperative-sticky | consumer   (consumer = the KIP-848 protocol)
#     MODE      kill (kill -9: the broker must notice by timeout) | stop (clean leave)
#
# Needs `scripts/lab_up.sh` and a topic `ex2` with 6 partitions.
set -euo pipefail
cd "$(dirname "$0")/.."

STRATEGY="${1:?strategy}"
MODE="${2:?kill|stop}"
LAB=(docker compose -f docker-compose.lab.yml --profile probe)
GROUP="g-$(openssl rand -hex 3)"
TMP="$(mktemp -d)"
trap 'docker rm -f probe-prod probe-c1 probe-c2 probe-c3 >/dev/null 2>&1 || true; rm -rf "$TMP"' EXIT

env_args=(-e "STRATEGY=$STRATEGY")
[[ "$STRATEGY" == "consumer" ]] && env_args=(-e PROTOCOL=consumer)

docker rm -f probe-prod probe-c1 probe-c2 probe-c3 >/dev/null 2>&1 || true
"${LAB[@]}" run -d --no-deps --name probe-prod lab-probe produce ex2 50 70 >/dev/null
for i in 1 2 3; do
  "${LAB[@]}" run -d --no-deps --name "probe-c$i" "${env_args[@]}" lab-probe consume ex2 "$GROUP" >/dev/null
  sleep 2
done
sleep 22 # let the group settle on its 3-way split

t_kill="$(date +%s.%N)"
if [[ "$MODE" == "kill" ]]; then docker kill probe-c3 >/dev/null; else docker stop -t 10 probe-c3 >/dev/null; fi
sleep 75 # the KIP-848 protocol's default session timeout (45 s) is longer than the classic one

echo "after the $MODE of c3 (t0 = the moment of the $MODE):"
for i in 1 2 3; do docker logs "probe-c$i" >"$TMP/c$i.jsonl" 2>&1 || true; done
python3 lab/gaps.py "$t_kill" "$TMP/c1.jsonl" "$TMP/c2.jsonl" "$TMP/c3.jsonl"
