# quant-platform — notes for Claude Code

**Self-hosted** event, audit trail and observability platform for the projects in
`~/`: first `quant-modeling` (derivatives pricing), then `data-ingest` and the
others. It provides a **Kafka** bus, an **append-only audit database**,
**telemetry** (OpenTelemetry, Prometheus, Loki, Tempo, Grafana) and **alerts**.

It is also a declared **learning project**: the maintainer wants to build what
quant desks build (event bus, audit trail, valuation governance). When the simple
choice and the "desk-like" choice diverge, take the second **if it is explained**,
and say honestly when a component is oversized for the volume (Kafka is).

**This repository contains no pricing code.** It does not contain the API
instrumentation either: that lives in `~/quant-modeling` (`api/app/audit/`). See
"Relationship with the other repositories".

## Repository status

The repository started from a design document written on the `quant-modeling` side
(`~/quant-modeling/blueprint/wp/18-observability.md`, which remains the overview of
both repositories). **Work packages 00 to 06 are built** (see the table in
[`blueprint/README.md`](blueprint/README.md)): foundations and deployment, Kafka,
audit database and sink, telemetry, data quality and alerts, schema registry,
exercise lab. The only **open** items are those each work package flags as depending
on a third party: the notification channel (maintainer's choice, ADR-013), exposing
Grafana through Cloudflare Access (manual action, `docs/RUNBOOK.md` §7), and the
cross-tests with the `quant-modeling` API (work packages 18a–18e). Assumptions about
producer payloads are in ADR-011 §6, to be confirmed on the `quant-modeling` side.

## Relationship with the other repositories

| Repository | Role relative to this one |
|---|---|
| `~/quant-modeling` | Event **producer** (FastAPI API). Owns the **payload schemas** and the *replay* endpoint. Its Claude session is separate from this one |
| `~/data-ingest` | **Market data** Postgres (`data-ingest-postgres`). Stays separate from the audit database. Future producer (ingestion freshness events) |
| `~/AgenticEnv` | Another project on the server; could eventually produce events |
| Docker network `dataplatform` | **Junction point**: shared external network. Whatever needs to talk connects to it by container name |

**Boundary rules, not to be bypassed:**

- **Never edit `~/quant-modeling` from a session opened here** (or the reverse). A
  change touching both sides means one issue in each repository, referencing each
  other, and two coordinated PRs.
- **The contract** between the two (topic names, event envelope, roles) is in
  [`docs/contract.md`](docs/contract.md). It is **canonical here**; any change is an
  ADR in [`blueprint/decisions.md`](blueprint/decisions.md).
- **Payloads** belong to the producer. This repository does not change a payload
  schema; it consumes them.

## Non-negotiable architecture principles

1. **Two parallel paths.** **Business events that must be accounted for**
   (valuations, authentication, fallbacks, assistant) go to **Kafka**. **Telemetry**
   (metrics, logs, traces) goes through **OpenTelemetry**, never through Kafka.
   Telemetry tolerates losing a sample; the audit trail does not.
2. **The API has no credentials for the audit database.** It produces to Kafka, full
   stop. Only `audit-sink` writes to the database. A compromised API can only add
   events, not rewrite history.
3. **The audit database is append-only.** Nobody has `UPDATE` or `DELETE`. Retention
   works by detaching then dropping a **whole monthly partition**, never row by row.
4. **At-least-once plus an idempotent sink** (key `event_id`), no end-to-end Kafka
   transactions.
5. **Kafka is not the archive**: the durable source of truth is Postgres.
6. **The platform being down never breaks the site.** The API falls back to a local
   spool. Nothing here may become a point of failure for pricing.
7. **Everything is code.** Topics, dashboards, alerts, SQL schemas, roles: versioned
   files, not configuration done by hand in an interface.
   `docker compose down -v && up -d` must rebuild everything.
8. **Cardinality**: never a ticker, a user, a `request_id` or an IP as a Prometheus or
   Loki **label**. Those values live in events and traces.
9. **No personal data in clear in the audit**: the IP is hashed (`HMAC-SHA256`) by the
   producer when emitting.

## Layout

```
docker-compose.yml        # the platform (external network `dataplatform`)
.env.placeholder          # copied to .env, never committed
topics/topics.yml         # topics as code + idempotent creation script
migrations/               # versioned SQL for the `audit` schema, roles, views
sink/                     # audit-sink (Python, confluent-kafka + psycopg)
data_quality/             # data quality consumer (work package 04)
otel/  prometheus/  loki/  tempo/
grafana/                  # data sources + provisioned dashboards
alerts/                   # alert rules as files
scripts/                  # deploy.sh, smoke.sh, make.sh…
docs/                     # contract.md, exercices.md, RUNBOOK.md
blueprint/                # work packages, decisions
```

Only create a directory when a work package fills it.

## Commands

| | |
|---|---|
| `scripts/bootstrap.sh`: creates `.venv` with the pinned tooling (`requirements-dev.txt`) | dev |
| `scripts/make.sh`: compose + Docker rules + yamllint + shellcheck + black + pytest | all |
| `scripts/init_env.sh` (`--sync`): creates `.env` with random secrets / adds missing variables | 00 |
| `scripts/up.sh`: dev, recomputes the configuration hashes, `up -d --wait` | 04 |
| `scripts/deploy.sh`: **from `~/quant-platform-prod` only** | 00 |
| `scripts/smoke.sh`: produces, reads back, compares; then Kafka → sink → Postgres | 01, 02 |
| `scripts/crash_test.sh`, `test_privileges.sh`, `audit_e2e.sh`, `test_migrations.sh`: criteria of work package 02 | 02 |
| `python3 scripts/verify_chain.py`: verifies the audit hash chain | 02 |
| `scripts/telemetry_e2e.sh`, `check_labels.sh`, `check_dashboards.py`, `measure_memory.sh` | 03 |
| `scripts/dq_crash_test.sh`, `dq_independence_test.sh`, `alert_e2e.sh` | 04 |
| `.venv/bin/python scripts/registry_test.py` | 05 |
| `scripts/lab_up.sh` / `lab_down.sh`: disposable lab (work package 06, never production) | 06 |
| `pytest`: unit tests (sink, `data-quality`, shared code) | 02+ |
| `pip-compile <file>.in`: regenerates the pinned `requirements*.txt` | 02 |

Integration scripts need the **development** stack running. The audit database is
append-only, so their test rows stay there (`docker compose down -v` resets the dev
stack; never on production).

## Tracking work: GitHub Issues, no handoff markdown

The project's "JIRA" is the repository's **GitHub Issues** (account `HadrienT`, `gh`
is authenticated).

- At the start of a session: `gh issue list --state open`.
- Issue handled → `gh issue close <n> --comment "done in <sha>"`.
- **Never** a markdown handoff file between sessions: a task that outlives the session
  is an issue.
- Large pieces of design live in `blueprint/wp/*.md`; the issue points to them, it does
  not replace them.

## Reference documents

| File | Role |
|---|---|
| [`blueprint/README.md`](blueprint/README.md) | Index of work packages, dependency graph, mapping to work packages 18a–18h of `quant-modeling` |
| [`blueprint/wp/`](blueprint/wp/) | One work package per file: scope, tasks, **acceptance criteria verifiable by a command** |
| [`blueprint/decisions.md`](blueprint/decisions.md) | ADRs: each choice with the alternatives rejected |
| [`docs/contract.md`](docs/contract.md) | The producer ↔ platform contract (topics, envelope, SQL roles) |
| `~/quant-modeling/blueprint/wp/18-observability.md` | Overview of both repositories (read-only from here) |

## Conventions

- **Language: English by default.** Everything you produce is in English: code, identifiers, comments, commit messages, PRs, issues, documentation and interface text. The only exception is the conversation: reply in the language the maintainer writes in, so in French when they write in French. `blueprint/` and `docs/` are historically in French: new content there is in English too.
- **Commits: go through a branch, never directly on `main`** (the repository's very
  first commit, which laid down the documentation, is the only exception). End each
  commit message with the `Co-Authored-By` line provided by the current Claude Code
  session.
- **A work package is not finished until its acceptance criteria are verified by the
  command that describes them.** "It seems to work" is not a criterion. If a test
  fails or a step is skipped, say so plainly.
- **Python**: `black`; dependencies declared in `requirements*.in`, pinned by
  `pip-compile` (as in `quant-modeling`), not installed by hand one at a time. **SQL**:
  numbered, replayable migrations, never changed after being applied (add a
  migration). **Shell**: `set -euo pipefail`, `shellcheck` clean. **YAML**: `yamllint`.
- **Docker**: **pinned** image versions (never `latest`); **explicit memory and CPU
  limits** on each service; **no port published outside `127.0.0.1`**; Kafka and the
  audit database are reachable only through the `dataplatform` network.
- **Secrets**: never in git. `.env` is ignored, `.env.placeholder` documents the
  variables.
- Every component that processes events comes with its **crash test** (kill the process
  mid-stream, restart, count: no loss, no duplicate). It is the equivalent here of the
  C++ core's property test.

## The maintainer: what to know before acting

- **They do not have deep knowledge of git or deployment.** Before any manipulation of
  branches, worktrees or merges, or any deployment, **explain** what each command does
  and what is reversible, in simple terms; prefer the simplest path (fast-forward, one
  clear command at a time). Once the plan is explained, they expect the work to be
  carried through to the end.
- **They want to learn.** Explain the *why* of each Kafka, SQL or observability concept
  when using it; suggest the exercises planned in the work packages.
- **Dependencies: declarative and turnkey**, no manual installs one by one (an isolated
  `apt-get install` is tolerated as an unblocker, flagged as such).
- **Shared hardware.** The server has ~56 cores and ~47 GB of RAM, but that RAM is
  shared with other projects and with the heavy computations of `quant-modeling`
  (~20 GB for an AAD run). Any sizing is **computed and measured** (`docker stats`), not
  guessed; hence the explicit caps.
- **Self-hosted, no cloud**: no managed service, no GCP.
- **Notifications**: the maintainer works from a Windows client over Remote-SSH to this
  headless server, and wants to be alerted (blocked, task finished) by a native
  notification, which requires a bridge off the machine (ntfy, Pushover…), to be chosen;
  see work package 04.

## Deployment

Same model as `quant-modeling`: two directories, **one git repository**
(`git worktree`).

- `~/quant-platform`: **development** (branches, commits, PRs). This is where Claude
  sessions work.
- `~/quant-platform-prod`: **the live platform, always on `main`**. Only used for
  `./scripts/deploy.sh` (`git pull --ff-only`, then `docker compose up -d`). **Never edit
  or commit from this directory.**

**Redeploying the API does not restart the platform, and vice versa.** At boot, the
platform starts first; the API starts without it (spool). A merge into `main` changes
nothing live until `deploy.sh` is run.

## What not to do

- Add pricing code, or read the `data-ingest` market database.
- Route telemetry through Kafka, or give the API access to the audit database.
- An `UPDATE` or a `DELETE` in the `audit` schema, even "just once".
- A dashboard or alert created by hand in the Grafana interface without bringing it back
  as a file.
- Publish a port on `0.0.0.0`, or expose Grafana / AKHQ publicly without Cloudflare
  Access.
- A high-cardinality metric label.
