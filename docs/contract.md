# Contrat producteur ↔ plateforme

Ce document est **canonique dans `quant-platform`**. Toute modification passe par
un ADR ([`../blueprint/decisions.md`](../blueprint/decisions.md)) et par une issue
dans chacun des dépôts concernés. Le producteur actuel est `quant-modeling`
(`api/app/audit/`) ; la conception côté producteur est dans son
`blueprint/wp/18-observability.md`.

Le contrat tient en trois choses : **l'enveloppe**, les **topics**, et les
**rôles de base de données**. Les **payloads** appartiennent au producteur ; l'enveloppe a un JSON Schema
exécutable : [`common/qp_common/envelope.schema.json`](../common/qp_common/envelope.schema.json).

## 1. Enveloppe (commune à tous les événements)

```json
{
  "event_id":    "0198f2c4-7b1e-7a3d-9f10-2a6c5e8d4b71",
  "type":        "pricing.valuation",
  "version":     1,
  "occurred_at": "2026-09-19T17:03:11.482Z",
  "request_id":  "req_01J8ZK3V9Q",
  "trace_id":    "4bf92f3577b34da6a3ce929d0e0e4736",
  "username":    "hadrien",
  "producer":    { "service": "quant-modeling-api", "git_sha": "6e251ca", "lib_build": "a13f09c" },
  "payload":     { }
}
```

| Champ | Règle |
|---|---|
| `event_id` | **UUID v7**, unique. Clé d'idempotence du puits : c'est ce qui transforme *at-least-once* en effet unique |
| `type` | `<domaine>.<sujet>`, ex. `pricing.valuation`, `auth.login_failed`, `data.fallback` |
| `version` | version du **schéma du payload** (pas de l'enveloppe) |
| `occurred_at` | horodatage UTC de l'événement, **posé par le producteur** (pas par le sink) ; sert à la partition mensuelle |
| `request_id` / `trace_id` | relient l'événement à la requête HTTP et à la trace OpenTelemetry |
| `username` | `null` pour l'anonyme |
| `producer` | qui a émis, avec quel build : indispensable au *replay* |
| `payload` | modèle typé du producteur ; **jamais** de mot de passe, jeton, en-tête `Authorization` |

**Aucune adresse IP en clair.** Le producteur émet `HMAC-SHA256(secret, ip)`.
La même IP donne le même hachage (détection de rafales), l'adresse n'est pas
récupérable depuis l'audit.

## 2. Topics

Convention : `qm.<domaine>.<sujet>.v<N>`. Un topic par sujet (un sujet = un
schéma + une rétention). Définis **en code** dans `topics/topics.yml`, créés de
façon idempotente par `scripts/`.

| Topic | Clé | Partitions | Rétention Kafka | Contenu |
|---|---|---|---|---|
| `qm.audit.valuation.v1` | `username` (ou `anon:<hash ip>`) | 3 | 90 j | une valorisation complète |
| `qm.audit.auth.v1` | `username` (ou `anon:<hash ip>`) | 3 | 90 j | `login_ok`, `login_failed`, `register`, `rate_limited`, `token_invalid` |
| `qm.dataquality.fallback.v1` | `kind` | 1 | 90 j | un repli ou une donnée périmée / proxifiée |
| `qm.http.access.v1` | `route` | 3 | 14 j | une requête HTTP (volumineux, faible valeur d'audit) |
| `qm.assistant.chat.v1` | `username` | 1 | 30 j | latence, script valide ou non ; **pas le contenu** par défaut |
| `qm.dlq.v1` | `source_topic` | 1 | 30 j | messages rejetés par un consommateur |

**Pourquoi la clé est `username` et pas `request_id`.** L'ordre qui compte pour
l'audit est celui des actions d'un même utilisateur (échecs en rafale, puis
succès). Kafka ne garantit l'ordre que **dans une partition** ; même clé → même
partition.

**Pourquoi 3 partitions sur certains topics et 1 sur d'autres.** Le volume ne le
justifie pas : c'est pour apprendre (ordre par clé, répartition entre
consommateurs d'un groupe, rééquilibrage vs ordre total).

**Messages rejetés (`qm.dlq.v1`).** Le consommateur qui rejette un message le
republie tel quel, avec en-têtes : `dlq.source.topic`, `dlq.source.partition`,
`dlq.source.offset`, `dlq.error`, `dlq.consumer.group`.

**Kafka n'est pas l'archive.** Rétention de 90 j : assez pour rejouer et
reconstruire. La source de vérité durable est la base d'audit.

## 3. Rôles et schéma de la base d'audit

Base `qm_audit`, schéma `audit`. Trois rôles, **personne n'a `UPDATE` ni `DELETE`** :

| Rôle | Droits | Utilisé par |
|---|---|---|
| `audit_owner` | DDL | migrations uniquement |
| `audit_writer` | `INSERT` sur `audit.events` | `audit-sink` uniquement |
| `audit_reader` | `SELECT` sur `audit.*` et les vues | Grafana, endpoint de *replay* de l'API |

```sql
CREATE TABLE audit.events (
    event_id     uuid        NOT NULL,
    type         text        NOT NULL,
    version      int         NOT NULL,
    occurred_at  timestamptz NOT NULL,
    request_id   text,
    trace_id     text,
    username     text,
    producer     jsonb       NOT NULL,
    payload      jsonb       NOT NULL,
    src_topic    text        NOT NULL,
    src_part     int         NOT NULL,
    src_offset   bigint      NOT NULL,
    PRIMARY KEY (event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);
```

Deux colonnes s'y ajoutent depuis la migration `0005` — `chain_prev bytea` et `chain_hash bytea`,
remplies par un déclencheur, `NULL` pour les lignes antérieures (ADR-015). Le sink ne les écrit pas
ni ne les lit : le contrat producteur ↔ plateforme n'en est pas modifié.

Partitions **mensuelles**. La rétention légale consiste à détacher puis
supprimer une partition entière — jamais un `DELETE` ligne à ligne.

## 4. Sémantique de livraison

- **Producteur** : `acks=all`, `enable.idempotence=true`, `linger.ms=20`,
  compression `zstd`, `produce()` non bloquant avec callback de livraison. Si le
  broker est indisponible ou l'acquittement échoue, l'événement retombe dans un
  **spool JSONL local** puis est republié. `emit()` ne lève jamais et ne bloque
  jamais la requête.
- **Consommateur** : *at-least-once*, `enable.auto.commit=false`, commit des
  offsets **après** le commit de la base ; insertion idempotente
  (`ON CONFLICT DO NOTHING`, sans cible : la clé primaire `(event_id, occurred_at)`
  est la seule contrainte d'unicité, et la forme avec cible exigerait `SELECT` en
  plus de `INSERT` — ADR-011).
- **Perte bornée assumée côté producteur** : un plantage de l'API entre la fin
  d'une requête et l'acquittement (au plus `linger.ms`) peut perdre des
  événements (ADR-006).

## 5. Compatibilité et évolution

- **Avant le lot 05** : les payloads sont décrits par des **JSON Schema** dans le dépôt
  producteur, validés par ses tests ; le sink **n'interprète pas** le payload (il le
  stocke en `jsonb`).
- **À partir du lot 05** : Avro + Apicurio Registry, mode **`BACKWARD`** posé globalement ;
  un schéma incompatible est refusé (HTTP 409, avec le champ fautif dans le message) et
  fait donc échouer la CI du producteur.
- **Les deux formats coexistent sur un même topic**, ce qui permet la migration sans
  « jour J » : le sink reconnaît le format au premier octet.
- Un changement **incompatible** = nouveau topic `…v2`, jamais une mutation de `…v1`.

### Format Avro sur le fil

La **valeur** du message est l'événement **entier** (champs de l'enveloppe du §1 + `payload`)
encodé en Avro, dans le cadrage standard de Confluent :

| Octets | Contenu |
|---|---|
| 0 | octet magique `0x00` (un message JSON commence par `{`, jamais par `0x00`) |
| 1–4 | identifiant du schéma dans le registre, entier big-endian |
| 5… | corps Avro, écrit avec ce schéma |

- **Sujet** = `<topic>-value` (un topic = un schéma, §2) : `qm.audit.valuation.v1-value`.
- Le schéma est un `record` dont les champs sont ceux de l'enveloppe (`event_id`, `type`,
  `version`, `occurred_at` en **chaîne** ISO 8601, `request_id`/`trace_id`/`username` en
  `["null","string"]` avec défaut `null`, `producer`, `payload`) ; **`payload` est un `record`
  défini par le producteur**. Exemple : [`../schemas/examples/valuation.v1.avsc`](../schemas/examples/valuation.v1.avsc).
- Les **règles d'évolution `BACKWARD`** : ajouter un champ **avec valeur par défaut** ; retirer
  un champ ; ne jamais ajouter un champ obligatoire sans défaut, ni changer le type d'un champ.
- Le sink décode avec le schéma **de l'écrivain** (identifiant du message), applique la même
  validation d'enveloppe qu'aux messages JSON, et stocke le `payload` en `jsonb` : la base
  d'audit ne change pas avec le format.
- Une panne du registre est **transitoire** (le sink réessaie, ne dead-lettre rien) ; un
  identifiant de schéma inconnu est **permanent** (DLQ).
- URL du registre depuis un conteneur du réseau `dataplatform` :
  `http://schema-registry:8080/apis/ccompat/v7` (API compatible Confluent).
