# WP 01 — Kafka

| | |
|---|---|
| **Dépend de** | [00](00-foundations.md) |
| **Bloque** | [02](02-audit-sink.md), [05](05-schema-registry.md) ; côté `quant-modeling`, le `KafkaSink` du lot 18b |
| **Branche** | `wp/01-kafka` |
| **Référence** | Shapira, Palino, Sivaram, Petty, *Kafka: The Definitive Guide*, 2ᵉ éd., O'Reilly 2021 (ch. 1–4) ; documentation de la version épinglée ; [`../decisions.md`](../decisions.md) ADR-001, 008, 010 |

## Objectif

Un broker Kafka **KRaft** joignable par le réseau `dataplatform`, avec ses topics
**définis en code**, une console pour regarder dedans, et un test de fumée.

## Ce qu'on apprend

| Notion | Où on la rencontre |
|---|---|
| **Topic, partition, clé** ; l'ordre n'est garanti **que dans une partition** | `topics.yml` : 3 partitions sur les topics à clé, 1 ailleurs |
| **Offset** : la position d'un consommateur dans une partition | `kafka-console-consumer` avec et sans `--from-beginning` |
| **KRaft** : le broker est aussi contrôleur, plus de ZooKeeper (retiré en Kafka 4.0) | la configuration du service |
| **Listeners** : pourquoi un client dans Docker et un client sur l'hôte n'utilisent pas la même adresse | `KAFKA_ADVERTISED_LISTENERS` |
| **Rétention** par topic | `topics.yml` |
| **Réplication** : ici RF = 1, donc **pas de haute disponibilité** — assumé (ADR-008) | `offsets.topic.replication.factor=1` |

## Tâches

1. **Service `kafka`** : image `apache/kafka:<tag épinglé>` (jamais `latest` ;
   choisir la dernière 4.x stable et **la noter dans le compose**), un seul nœud
   `broker,controller`, volume nommé pour les données.
   - Deux *listeners* : `INTERNAL` (`kafka:9092`, réseau `dataplatform`) pour les
     conteneurs, et `EXTERNAL` lié à **`127.0.0.1` seulement** pour déboguer
     depuis l'hôte. **Aucun port publié sur `0.0.0.0`.**
   - `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m` : sans plafond la JVM prend une fraction
     de la RAM de la machine, qui est partagée.
   - Limites Docker : `mem_limit: 1g`, `cpus` explicites.
   - `auto.create.topics.enable=false` : les topics sont **du code**, un producteur
     qui se trompe de nom doit échouer, pas créer un topic fantôme.
   - Réplication des topics internes à 1 (`offsets.topic.replication.factor`,
     `transaction.state.log.replication.factor`, `transaction.state.log.min.isr`).
   - `healthcheck` : `kafka-broker-api-versions.sh --bootstrap-server localhost:9092`.
   - Les noms exacts des variables d'environnement de l'image sont **à vérifier
     dans la documentation de la version épinglée**, pas à recopier de mémoire.
2. **Service `akhq`** (Apache 2.0), lié à `127.0.0.1:${QP_AKHQ_PORT}`, configuré par
   fichier versionné.
3. **`topics/topics.yml`** : les six topics de
   [`../../docs/contract.md`](../../docs/contract.md) §2 (nom, partitions,
   `retention.ms`, `cleanup.policy`).
4. **`scripts/topics.sh`** : lit `topics.yml` et **aligne** le broker de façon
   idempotente (`--create --if-not-exists`, puis `kafka-configs.sh --alter` pour la
   rétention). Le lancer deux fois ne change rien. Il **refuse** de réduire un
   nombre de partitions (Kafka ne sait pas) et le dit.
5. **`scripts/smoke.sh`** : produit un événement d'enveloppe valide sur
   `qm.audit.valuation.v1` avec `kafka-console-producer`, le relit avec
   `kafka-console-consumer`, compare. Sort en 0 ou non-zéro.
6. **Côté `quant-modeling`** (dépôt séparé, autre session) : le `KafkaSink` du lot
   18b s'y branche. Ouvrir/référencer l'issue correspondante.

## Critères d'acceptation

- `docker compose up -d` puis `scripts/smoke.sh` sort en 0.
- `scripts/topics.sh` lancé **deux fois** : la seconde ne modifie rien.
- `docker compose exec kafka kafka-topics.sh --describe` montre les six topics avec
  les bonnes partitions et rétentions.
- `docker stats` : le conteneur Kafka reste sous son plafond ; le noter dans
  [`../README.md`](../README.md) §5.
- `ss -ltn` sur l'hôte : aucun port Kafka/AKHQ à l'écoute ailleurs que sur
  `127.0.0.1`.
- Produire sur un topic **inexistant** échoue (preuve que `auto.create` est coupé).
- **Test de résilience croisé** (quand `quant-modeling` 18b existe) : broker arrêté,
  100 événements émis par l'API (aucune requête ne ralentit), 100 dans le spool ;
  broker relancé, les 100 arrivent et le spool se vide.

## Exercices

1. Créer un topic à 1 puis à 3 partitions, produire des messages de **clés
   différentes**, et voir dans AKHQ dans quelle partition chacun atterrit. Les
   messages de même clé arrivent-ils toujours au même endroit ?
2. Consommer avec `--from-beginning` puis sans, avec deux groupes différents :
   comprendre ce qu'est un *offset* et pourquoi deux groupes ne se volent pas les
   messages.
3. Arrêter le broker, le relancer : les messages sont-ils toujours là ? (Volume
   nommé.) Refaire avec `docker compose down -v` : que se passe-t-il ?

## Fichiers créés

`docker-compose.yml` (services `kafka`, `akhq`), `topics/topics.yml`,
`scripts/topics.sh`, `scripts/smoke.sh`, `akhq/application.yml`.
