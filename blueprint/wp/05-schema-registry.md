# WP 05 — Registre de schémas et Avro *(optionnel)*

| | |
|---|---|
| **Dépend de** | [01](01-kafka.md) ; idéalement après [02](02-audit-sink.md) |
| **Bloque** | rien (profondeur, pas chemin critique) |
| **Branche** | `wp/05-schema-registry` |
| **Référence** | *Kafka: The Definitive Guide* ch. 3 (sérialisation, Avro, registre) ; Kleppmann, *DDIA* ch. 4 (encodage et évolution) ; [`../../docs/contract.md`](../../docs/contract.md) §5 ; ADR-007 |

## Objectif

Passer d'un contrat **par convention** (JSON Schema dans le dépôt producteur,
validé par ses tests) à un contrat **par registre** : le producteur enregistre son
schéma, le consommateur le lit, et une modification incompatible **échoue en CI**.
Le registre est littéralement l'endroit où deux dépôts se rencontrent.

## Ce qu'on apprend

| Notion | Où |
|---|---|
| **Avro** : schéma séparé des données, encodage compact, valeurs par défaut | schémas des payloads |
| **Registre** : un identifiant de schéma dans chaque message | producteur et consommateurs |
| **Modes de compatibilité** : `BACKWARD`, `FORWARD`, `FULL` — qui doit pouvoir lire quoi | test de CI |
| Pourquoi ajouter un champ **sans valeur par défaut** casse les anciens lecteurs | l'exercice guidé |

## Tâches

1. **Service Apicurio Registry** (Apache 2.0, version épinglée, `mem_limit`), API
   compatible Confluent, sur `dataplatform`, `127.0.0.1` pour la console.
   **Test de fumée en tout premier** : la couche de compatibilité Confluent
   d'Apicurio a des particularités ; vérifier qu'elle fonctionne avec
   `confluent-kafka-python` **avant** d'investir. Si elle gêne, l'alternative est
   **Karapace** (Apache 2.0, même API) — noter l'ADR.
2. **Convertir les payloads en Avro** *(avec le dépôt producteur : issue croisée)* ;
   le sink, qui stocke aujourd'hui le payload en `jsonb` sans l'interpréter,
   désérialise via l'identifiant de schéma et **continue** de stocker en `jsonb`.
3. **Compatibilité `BACKWARD` par sujet**, imposée par le registre.
4. **Test de CI côté producteur** qui refuse un schéma incompatible (ajouter un champ
   obligatoire sans défaut, par exemple). Ce test est livré dans `quant-modeling`.
5. **Documenter** dans [`../../docs/contract.md`](../../docs/contract.md) §5 le passage
   à Avro et les règles de nommage des sujets.

## Critères d'acceptation

- Un producteur de test enregistre un schéma ; le sink le lit dans le registre et
  insère l'événement.
- **Exercice d'évolution** : ajouter au payload de `qm.audit.valuation.v1` un champ
  **avec valeur par défaut** ; un message ancien reste lisible par le nouveau
  consommateur (compatibilité `BACKWARD`).
- Enregistrer un schéma **incompatible** est refusé par le registre, avec un message
  qui nomme le champ fautif.
- Les messages déjà présents dans Kafka **avant** la bascule restent lisibles (ou la
  bascule passe par un topic `…v2`, ce qui est la règle du contrat pour un
  changement incompatible).

## Exercices

1. Casser volontairement la compatibilité, voir l'erreur du registre, la corriger.
2. Comparer la taille d'un même événement en JSON et en Avro sur 10 000 messages.

## Fichiers créés

`docker-compose.yml` (service `apicurio`), `schemas/` *(si les schémas sont
centralisés ici plutôt que chez le producteur : décision à prendre et à noter en
ADR)*, mise à jour de `sink/` et de `docs/contract.md`.
