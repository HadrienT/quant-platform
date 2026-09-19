# WP 06 — Exercices d'opérateur

| | |
|---|---|
| **Dépend de** | [02](02-audit-sink.md) |
| **Bloque** | rien |
| **Branche** | `wp/06-operator-exercises` |
| **Référence** | *Kafka: The Definitive Guide* ch. 6–7 (réplication, opérations) et ch. 11 (sécurité) ; documentation de la version épinglée |

## Objectif

Un lot de **pratique** : sans livrable applicatif, il apprend ce que le mode
« un seul broker, sans haute disponibilité » (ADR-008) cache. Tout se fait dans un
**environnement jetable** (compose séparé, `docker-compose.lab.yml`), **jamais** sur
la plateforme de production.

Le livrable est un document, `docs/exercices.md` : pour chaque exercice, ce qu'on a
fait, ce qu'on a **observé**, et ce qu'on en retient — avec les commandes.

## Règle de sécurité du lot

Les exercices tuent des brokers et réécrivent des offsets. Ils s'exécutent dans un
projet Compose **distinct** (`name: quant-platform-lab`) avec ses propres volumes et
son propre réseau. Avant de lancer quoi que ce soit, **vérifier qu'on n'est pas
branché sur `dataplatform`**.

## Exercices

1. **Grappe de 3 brokers en KRaft**, RF = 3, `min.insync.replicas=2`.
   - Tuer un broker : la production continue.
   - Tuer un second : elle s'arrête (`acks=all` exige 2 réplicas synchrones).
   - Relancer : observer le rattrapage et la liste des ISR
     (`kafka-topics.sh --describe`).
   - *À retenir :* ce que RF, ISR et `min.insync.replicas` garantissent, et ce
     qu'ils coûtent en disponibilité.
2. **Rééquilibrage** : trois instances d'un consommateur dans un groupe ; en tuer
   une ; regarder les partitions migrer et mesurer la pause de consommation.
   Comparer les stratégies d'affectation (*range*, *cooperative sticky*).
3. **SASL/SCRAM et ACL** : activer l'authentification, créer un utilisateur
   `qm-api` qui peut **seulement produire** sur `qm.*`, et un utilisateur pour le
   sink qui peut **seulement consommer**. Vérifier qu'un producteur non autorisé est
   refusé.
4. **Compaction** : un topic `cleanup.policy=compact` qui garde le dernier état par
   clé ; produire des mises à jour, attendre la compaction, constater. Comprendre
   pourquoi ce n'est **pas** adapté à une piste d'audit.
5. **Rejeu à grande échelle** : produire un million d'événements, mesurer le débit du
   sink, faire varier la taille de lot, voir où est le goulot (le broker, la base, ou
   le sink).
6. **Perte de disque** : arrêter le broker, supprimer son volume, relancer, et
   comparer ce qui est perdu en mode 1 broker vs grappe de 3. C'est la justification
   concrète de la haute disponibilité.

## Critères d'acceptation

- `docs/exercices.md` contient, pour chaque exercice, les commandes lancées et les
  **observations réelles** (sorties collées), pas la théorie recopiée.
- Le lab se démarre et se détruit avec deux commandes, sans laisser de conteneur ni
  de volume sur la machine (vérifié par `docker ps -a` et `docker volume ls`).
- La plateforme de production n'a **jamais** été touchée (ses conteneurs n'ont pas
  redémarré pendant le lot).

## Fichiers créés

`docker-compose.lab.yml`, `docs/exercices.md`, `scripts/lab_up.sh`,
`scripts/lab_down.sh`.
