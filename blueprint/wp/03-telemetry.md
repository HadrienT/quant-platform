# WP 03 — Télémétrie

| | |
|---|---|
| **Dépend de** | [00](00-foundations.md) (indépendant de Kafka : c'est l'autre chemin, ADR-004) |
| **Bloque** | [04](04-data-quality-alerts.md) |
| **Branche** | `wp/03-telemetry` |
| **Référence** | Majors, Fong-Jones, Miranda, *Observability Engineering*, O'Reilly 2022 ; documentation OpenTelemetry (Collector, sémantique des conventions) ; ADR-004 |

## Objectif

Métriques, logs et traces de l'API et de la plateforme elle-même, dans **Grafana**,
avec des tableaux de bord **provisionnés depuis des fichiers** : `docker compose
down -v && up -d` doit tout reconstruire.

## Ce qu'on apprend

| Notion | Où |
|---|---|
| Les trois piliers : **métriques, logs, traces** — ce que chacun sait et ne sait pas dire | les quatre tableaux de bord |
| **Méthode RED** (Rate, Errors, Duration) | tableau de bord API |
| **Cardinalité** : la cause n°1 des pannes de Prometheus et de Loki | la règle d'étiquettes |
| Le **Collector** comme couture : changer de backend sans toucher l'application | `otel/collector.yml` |
| **Corrélation** : d'une ligne d'audit à sa trace par `trace_id` | Grafana |
| **Retard de consommation** (*consumer lag*) comme métrique de santé d'un pipeline | tableau de bord Kafka |

## Tâches

1. **Services** (versions épinglées, `mem_limit` explicites, `127.0.0.1` seulement) :
   OTel Collector, Prometheus, Loki, Tempo, Grafana, et un **exporteur Kafka** pour
   le retard de consommation et le débit par topic (`kafka-exporter`, Apache 2.0,
   ou JMX — choisir et noter l'ADR).
2. **Collector** : récepteur OTLP (gRPC 4317) sur `dataplatform` ; pipelines
   métriques → Prometheus, logs → Loki, traces → Tempo. Les logs des conteneurs
   (dont l'API) sont collectés par le Collector (récepteur `filelog` sur les
   journaux Docker, monté en lecture seule) ou par Grafana Alloy : choisir un seul
   agent et noter l'ADR.
3. **Rétention** : Loki 30 jours (c'est là que vit l'IP en clair des logs
   d'accès, voir ADR-009), Tempo quelques jours, Prometheus 15 jours.
4. **Grafana provisionné** : sources de données (Prometheus, Loki, Tempo,
   `qm-audit` en `audit_reader`) et tableaux de bord dans `grafana/` **en JSON
   versionné**. Aucun tableau de bord créé à la main sans être rapatrié.
5. **Quatre tableaux de bord** :
   - *API* : débit, erreurs, durées (RED), par route normalisée ;
   - *Pricing* : histogrammes de durée par produit, moteur, modèle ;
   - *Kafka* : débit par topic, **retard par groupe**, état du sink ;
   - *Sécurité* : échecs de connexion, `rate_limited`, par IP hachée (depuis
     `qm-audit`).
6. **Relevé de mémoire réel** (`docker stats`, plusieurs minutes de charge) : mettre à
   jour [`../README.md`](../README.md) §5 avec les mesures, et ajuster les plafonds.
7. **Exposition de Grafana** derrière le tunnel Cloudflare : demande d'ajouter un
   nom d'hôte dans le tableau de bord Cloudflare (le tunnel y est géré) et de le
   protéger par **Cloudflare Access**. C'est une **action manuelle du mainteneur** :
   la lui expliquer étape par étape avant de la demander. Ne rien exposer
   publiquement tant qu'elle n'est pas faite.

## Contrat de métriques avec le producteur

Les tableaux de bord dépendent de noms **stables**, émis par l'API (préfixe `qm_`) :

| Métrique | Type | Étiquettes |
|---|---|---|
| `qm_pricing_duration_seconds` | histogramme | `product`, `engine`, `model` |
| `qm_pricing_errors_total` | compteur | `product`, `code` |
| `qm_data_fallback_total` | compteur | `kind` |
| `qm_auth_events_total` | compteur | `outcome` |
| `qm_audit_dropped_total`, `qm_audit_spool_depth` | compteur, jauge | — |

**Règle de cardinalité** : **jamais** un ticker, un utilisateur, un `request_id` ou
une IP comme étiquette. Si un tableau de bord a besoin d'une telle valeur, elle
vient de `qm-audit` (SQL) ou d'une trace, pas d'une étiquette de métrique.

## Critères d'acceptation

- `docker compose down -v && docker compose up -d` reconstruit **les mêmes**
  tableaux de bord et sources de données, sans intervention.
- Depuis une ligne de `audit.events`, on ouvre la trace correspondante dans Grafana
  par `trace_id` (nécessite `quant-modeling` lot 18d).
- Le **retard** du groupe `audit-sink` est affiché, **monte** quand on arrête le
  sink, et **retombe** quand on le relance.
- Aucun port publié hors `127.0.0.1` ; Grafana n'est pas joignable publiquement
  avant l'étape 7.
- Les plafonds de mémoire du README reflètent des **mesures**, pas des estimations.
- Un test (script) vérifie qu'aucune métrique n'expose une étiquette interdite.

## Exercices

1. Provoquer volontairement une explosion de cardinalité (une étiquette
   `request_id` sur une métrique de test) et observer la mémoire de Prometheus
   monter ; comprendre pourquoi la règle existe.
2. Une requête lente : la retrouver de la métrique (p95) à la trace, puis à la ligne
   d'audit correspondante.

## Fichiers créés

`docker-compose.yml` (services de télémétrie), `otel/collector.yml`,
`prometheus/prometheus.yml`, `loki/config.yml`, `tempo/config.yml`,
`grafana/provisioning/`, `grafana/dashboards/*.json`, `scripts/check_labels.sh`.
