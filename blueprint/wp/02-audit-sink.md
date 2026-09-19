# WP 02 — Base d'audit et puits (`audit-sink`)

| | |
|---|---|
| **Dépend de** | [01](01-kafka.md) |
| **Bloque** | [04](04-data-quality-alerts.md), [06](06-operator-exercises.md) ; côté `quant-modeling`, le *replay* (lot 18e) |
| **Branche** | `wp/02-audit-sink` |
| **Référence** | Kleppmann, *Designing Data-Intensive Applications*, O'Reilly 2017, ch. 11 (journal, idempotence) ; *Kafka: The Definitive Guide* ch. 4 (consommateurs, offsets) ; [`../../docs/contract.md`](../../docs/contract.md) §3–4 ; ADR-003, 005 |

## Objectif

La **piste d'audit durable** : un Postgres dédié, append-only, alimenté par un
consommateur Kafka qui ne perd rien et ne duplique rien, même s'il plante.

## Ce qu'on apprend

| Notion | Où |
|---|---|
| **Groupe de consommateurs**, offsets, commit **manuel** | le sink |
| **Sémantique de livraison** : *at-least-once* + puits idempotent = effet unique | `ON CONFLICT DO NOTHING` sur `event_id` |
| **Message empoisonné** vs **panne transitoire** : ne pas confondre | DLQ vs *retry* |
| **Kafka n'est pas l'archive** ; le journal est **rejouable** | test de reconstruction |
| **Immuabilité** par les privilèges SQL, pas par la confiance | rôles sans `UPDATE`/`DELETE` |
| **Partitionnement** SQL et rétention par détachement de partition | partitions mensuelles |

## Tâches

### Base

1. **Service `qm-audit`** : Postgres (version épinglée, 16 ou 17), base `qm_audit`,
   volume nommé, `mem_limit`, joignable sur `dataplatform` seulement (et
   `127.0.0.1` pour déboguer). **Distinct** du Postgres de `data-ingest`.
2. **`migrations/`** : SQL numéroté (`0001_schema.sql`, …), rejouable, **jamais
   modifié après application** (on ajoute une migration). Un petit *runner* les
   applique dans l'ordre et journalise ce qui a été appliqué.
3. **Schéma** : la table `audit.events` de
   [`contract.md`](../../docs/contract.md) §3, partitionnée par mois. Ajouter une
   **partition `DEFAULT`** : sans elle, un événement dont le mois n'a pas de
   partition ferait échouer l'insertion et **bloquerait le sink** — un
   événement mal daté ne doit pas arrêter l'audit.
4. **Maintenance des partitions** : une tâche (timer systemd ou service) crée les
   trois mois suivants à l'avance ; une autre **détache puis supprime** les
   partitions au-delà de la rétention (par défaut 12 mois pour les valorisations,
   90 jours pour `http.access`). Pas de `DELETE` ligne à ligne.
5. **Trois rôles** (`audit_owner`, `audit_writer`, `audit_reader`) avec exactement
   les droits du contrat. Personne n'a `UPDATE` ni `DELETE`.
6. **Vues** : `v_fallbacks_daily`, `v_login_failures_by_hash`,
   `v_slowest_valuations`, `v_valuations_by_status` (un input `default` ou
   `proxied` compte comme non-observé).
7. **Sauvegarde** : timer systemd (`pg_dump`) sur le modèle du timer de backup de
   `quant-modeling`, vers un volume ou un chemin hors du conteneur. *Une piste
   d'audit qu'on ne sauvegarde pas n'en est pas une.*

### Puits

8. **`sink/`** — Python, `confluent-kafka` + `psycopg` ; dépendances dans
   `requirements.in`, figées par `pip-compile` ; image Docker dédiée ; `black`.
   - Groupe `audit-sink`, `enable.auto.commit=false`, abonné aux topics
     `qm.audit.*`, `qm.dataquality.*`, `qm.http.access.v1`, `qm.assistant.*`.
   - **Lecture par lots** (N messages ou T ms, le premier atteint) ; **une
     transaction par lot** : `INSERT … ON CONFLICT (event_id, occurred_at) DO
     NOTHING`, puis **commit des offsets après le commit de la base**.
   - **Valide l'enveloppe** (JSON Schema de l'enveloppe uniquement — il
     **n'interprète pas le payload**, ADR-007). Un message illisible ou qui viole
     l'enveloppe part dans `qm.dlq.v1` avec les en-têtes `dlq.*` du contrat, puis
     on avance : **jamais de boucle infinie sur un message empoisonné**.
   - **Distinguer** l'erreur *transitoire* (base injoignable) de l'erreur
     *permanente* (message invalide). Transitoire : **on ne commit pas, on ne met
     pas en DLQ**, on réessaie avec un recul exponentiel. Envoyer en DLQ pendant
     une panne de la base viderait la piste d'audit dans la poubelle.
   - **Arrêt propre** sur `SIGTERM` : finir le lot, commiter, quitter. Sur
     rééquilibrage (*revoke*), commiter avant de rendre les partitions.
   - Écrit dans les colonnes `src_topic`, `src_part`, `src_offset`.
9. **Chaîne de hachage** *(optionnelle, en dernier)* : par `(topic, partition)`,
   chaque ligne stocke `sha256(hash_précédent ‖ contenu)` ; une modification
   rétroactive casse la chaîne. Un seul écrivain par partition la rend
   déterministe. Un script `verify_chain` la vérifie.

## Critères d'acceptation

- **Test de plantage** — `scripts/crash_test.sh` : produire 1 000 événements, tuer
  `audit-sink` (`kill -9`) au milieu, le relancer, attendre la fin, compter :
  **exactement 1 000 lignes, aucun doublon** (`SELECT count(*), count(DISTINCT
  event_id)`). Ce script est le critère, pas une démonstration.
- **Test de privilèges** — un script SQL automatisé : `audit_writer` et
  `audit_reader` qui tentent `UPDATE` ou `DELETE` reçoivent une erreur de
  privilège ; `audit_reader` qui tente `INSERT` aussi.
- **DLQ** — un message corrompu envoyé à la main atterrit dans `qm.dlq.v1` avec
  ses en-têtes, et le sink continue de consommer derrière.
- **Panne de la base** — arrêter `qm-audit` pendant que des événements arrivent :
  rien dans la DLQ, les événements sont insérés au redémarrage de la base, sans
  doublon.
- **Reconstruction** — vider la table, remettre l'offset du groupe à zéro
  (`kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`), relancer :
  la base retrouve son contenu (dans la limite de la rétention Kafka). C'est la
  démonstration que le journal est rejouable.
- **Événement mal daté** (mois sans partition) : inséré dans la partition
  `DEFAULT`, le sink ne s'arrête pas.
- Les migrations rejouées sur une base vide donnent le schéma attendu ; rejouer
  n'applique rien de nouveau.

## Exercices

1. Lancer **deux** instances du sink dans le même groupe : voir les partitions se
   répartir dans AKHQ ; en tuer une, voir la réaffectation.
2. Lancer un **second groupe** sur les mêmes topics (sans écrire nulle part) : il
   reçoit tout, indépendamment. C'est le principe qu'exploite le lot 04.
3. Retarder volontairement le commit après l'insertion (`sleep`) et tuer le sink :
   observer la relecture du lot et l'absence de doublon.

## Fichiers créés

`docker-compose.yml` (services `qm-audit`, `audit-sink`), `migrations/*.sql`,
`sink/` (code, `requirements.in`, `requirements.txt`, `Dockerfile`, tests),
`scripts/crash_test.sh`, `scripts/verify_chain.py` (option), timers systemd de
maintenance et de sauvegarde.
