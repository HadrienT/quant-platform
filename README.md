# quant-platform

Self-hosted event bus, append-only audit trail and observability stack for the
quant projects that run on this server (first `quant-modeling`, then
`data-ingest` and others).

| Concern | Component |
|---|---|
| Business events (valuations, auth, fallbacks) | Apache Kafka (KRaft) |
| Audit trail | dedicated Postgres, append-only, monthly partitions |
| Metrics / logs / traces | OpenTelemetry Collector → Prometheus / Loki / Tempo |
| Dashboards and alerts | Grafana, provisioned from files |
| Schema contract | Apicurio Registry + Avro (optional, WP 05) |

**Status:** built — work packages 00–06 delivered (see
[`blueprint/README.md`](blueprint/README.md)). Start with
[`docs/RUNBOOK.md`](docs/RUNBOOK.md) to bring it up; the producer/platform contract is
in [`docs/contract.md`](docs/contract.md); design decisions with the alternatives
rejected are in [`blueprint/decisions.md`](blueprint/decisions.md); learning exercises
are in [`docs/exercices.md`](docs/exercices.md). Working notes for Claude Code are in
[`CLAUDE.md`](CLAUDE.md).

Quick start (development checkout):

```bash
scripts/bootstrap.sh && scripts/init_env.sh   # tooling + a .env with random secrets
scripts/up.sh                                 # the whole platform
scripts/smoke.sh                              # produce -> Kafka -> sink -> Postgres
```

The platform joins other projects through the external Docker network
`dataplatform`; nothing here is published outside `127.0.0.1`.
