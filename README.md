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

**Status:** design phase — documentation only. Start with
[`blueprint/README.md`](blueprint/README.md); the producer/platform contract is
in [`docs/contract.md`](docs/contract.md). Working notes for Claude Code are in
[`CLAUDE.md`](CLAUDE.md).

The platform joins other projects through the external Docker network
`dataplatform`; nothing here is published outside `127.0.0.1`.
