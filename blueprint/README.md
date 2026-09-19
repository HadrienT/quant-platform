# Blueprint — quant-platform

Spécification du chantier. Un fichier par **lot de travail** dans
[`wp/`](wp/), les arbitrages avec leurs alternatives rejetées dans
[`decisions.md`](decisions.md), le contrat avec les producteurs dans
[`../docs/contract.md`](../docs/contract.md).

> **Origine.** Ce dépôt est né du lot 18 de `quant-modeling`
> (`~/quant-modeling/blueprint/wp/18-observability.md`), qui reste la **vue
> d'ensemble** des deux dépôts et décrit aussi la moitié « producteur ». Ici, on
> ne détaille que la moitié « plateforme ». En cas de divergence : le **contrat**
> est canonique ici ; le **code producteur** est canonique là-bas.

## 1. Pourquoi ce dépôt existe

Le projet `quant-modeling` a des replis silencieux sur des sources en direct,
aucun journal d'authentification, et ne peut pas répondre à « de quoi dépendait
ce prix mardi, et retrouve-t-on le même aujourd'hui ? ». La réponse est une
**piste d'audit** et une **télémétrie**. Elles sont ici, et pas dans le dépôt de
pricing, parce qu'une plateforme a un autre cycle de vie (redéployer l'API ne
doit pas redémarrer Kafka) et qu'elle servira à d'autres projets.

**Règle qui départage « même dépôt » et « dépôt séparé »** : deux choses vivent
dans le même dépôt si elles changent ensemble et doivent être livrées de façon
atomique ; sinon elles se parlent par un contrat versionné.

## 2. Deux buts, qui ne pèsent pas pareil

Le premier est pratique : voir les replis, les connexions, les pricings. Le
second est **pédagogique et explicite** : apprendre à monter ce qu'un desk monte.
Quand ils divergent — Kafka est surdimensionné pour ce volume, Postgres seul
suffirait — c'est le second qui gagne, et la documentation le dit.

## 3. Les lots

| WP | Titre | Correspond au lot de `quant-modeling` | Résumé |
|---|---|---|---|
| [00](wp/00-foundations.md) | Fondations | — | Squelette du dépôt, compose de base, CI, `deploy.sh`, dossier de prod |
| [01](wp/01-kafka.md) | Kafka | 18b (moitié plateforme) | Broker KRaft, AKHQ, topics en code, test de fumée |
| [02](wp/02-audit-sink.md) | Base d'audit et puits | 18c | Postgres append-only, `audit-sink`, DLQ, reconstruction |
| [03](wp/03-telemetry.md) | Télémétrie | 18d (moitié plateforme) | OTel Collector, Prometheus, Loki, Tempo, Grafana en code |
| [04](wp/04-data-quality-alerts.md) | Qualité des données et alertes | 18f (moitié plateforme) | Second consommateur, règles d'alerte, canal de notification |
| [05](wp/05-schema-registry.md) | Registre de schémas | 18g | Apicurio, Avro, compatibilité `BACKWARD` *(optionnel)* |
| [06](wp/06-operator-exercises.md) | Exercices d'opérateur | 18h | Grappe de 3 brokers, rééquilibrage, SASL/ACL, compaction, rejeu |

Les lots **18a** (socle applicatif) et **18e** (reproductibilité et *replay*)
n'ont pas de moitié ici : ils sont entièrement dans `quant-modeling`.

## 4. Graphe de dépendances

```mermaid
graph TD
    W0[00 · Fondations]
    W1[01 · Kafka]
    W2[02 · Base d'audit et puits]
    W3[03 · Télémétrie]
    W4[04 · Qualité et alertes]
    W5[05 · Registre de schémas]
    W6[06 · Exercices]
    P18a[["quant-modeling 18a<br/>socle applicatif"]]
    P18e[["quant-modeling 18e<br/>replay"]]

    W0 --> W1 --> W2
    W0 --> W3
    W2 --> W4
    W3 --> W4
    W1 --> W5
    W2 --> W6
    P18a -. "émet des événements" .-> W1
    W2 -.-> P18e
```

Traits pointillés : dépendances **entre dépôts**. Le lot 01 est testable de bout
en bout dès que `quant-modeling` 18a (spool) et 18b (`KafkaSink`) existent ; d'ici
là, `scripts/smoke.sh` produit des événements de test à la main.

**Ordre conseillé** : 00 → 01 → 02 (on a alors la piste d'audit complète), puis
03, puis 04. Les lots 05 et 06 sont de la profondeur, pas du chemin critique.

## 5. Empreinte mémoire (estimation, à mesurer au lot 03)

Plafonds explicites : la RAM de la machine est partagée avec d'autres projets et
avec les calculs lourds de `quant-modeling`.

| Service | Plafond | | Service | Plafond |
|---|---|---|---|---|
| Kafka | 1 Go | | Prometheus | 512 Mo |
| Apicurio Registry | 512 Mo | | Loki | 512 Mo |
| AKHQ | 256 Mo | | Tempo | 384 Mo |
| Postgres `qm-audit` | 512 Mo | | Grafana | 256 Mo |
| `audit-sink`, `data-quality` | 128 Mo chacun | | OTel Collector | 128 Mo |

≈ 4 Go de **plafonds cumulés**, pour un usage réel probablement autour de 2 Go.
Ce sont des estimations : le lot 03 relève l'usage réel (`docker stats`) et
ajuste, il ne les laisse pas à l'intuition.

## 6. Règles qui s'appliquent à tous les lots

- Un lot n'est **pas** terminé tant que ses critères d'acceptation ne sont pas
  vérifiés par la commande qui les décrit.
- Chaque lot est une branche et au moins une issue GitHub (voir
  [`../CLAUDE.md`](../CLAUDE.md)) ; l'issue référence son fichier de lot.
- Tout est du code (topics, dashboards, alertes, SQL, rôles) — voir les principes
  d'architecture de `CLAUDE.md`.
- Chaque composant qui traite des événements a son **test de plantage**.
- Prose de ce dossier en français ; code, identifiants et messages de commit en
  anglais.
