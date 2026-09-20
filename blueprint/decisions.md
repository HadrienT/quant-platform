# Décisions d'architecture

Chaque décision : ce qui est retenu, pourquoi, et **ce qui est écarté**. Une
décision qui change devient une nouvelle entrée qui la remplace ; on ne réécrit
pas l'historique.

Numérotation locale (ADR-001…). La correspondance avec les décisions D1–D9 du lot
18 de `quant-modeling` est indiquée entre parenthèses.

---

## ADR-001 — Kafka, malgré le volume (D1)

**Décision.** Le bus d'événements est **Apache Kafka 4.x** (image officielle
`apache/kafka`, mode KRaft, version épinglée au lot 01).

**Pourquoi.** À ce volume (quelques événements par seconde au pire), Postgres
seul suffirait, et ce serait plus simple. Kafka est un **choix d'apprentissage**
assumé. Il apporte pourtant des propriétés réelles : découplage temporel (une
panne de la base d'audit ne ralentit pas un pricing), journal **rejouable**,
plusieurs consommateurs indépendants sur un même flux, ordre par clé, et c'est un
standard des desks.

**Écarté.**
- *Postgres seul* (`LISTEN/NOTIFY`, table de file) : suffisant en volume, mais on
  n'y apprend ni le journal rejouable, ni les groupes, ni l'évolution de schéma.
- *Redpanda* : compatible Kafka, plus léger, mais licence BSL, et l'objectif est
  d'apprendre Kafka lui-même.
- *RabbitMQ / NATS* : bons courtiers de messages, mais pas un journal rejouable
  au même sens.

---

## ADR-002 — Deux dépôts, un contrat (D2)

**Décision.** L'infrastructure vit dans ce dépôt ; le code producteur et les
schémas de payload restent dans `quant-modeling`. Le contrat
([`../docs/contract.md`](../docs/contract.md)) est canonique ici.

**Pourquoi.** Le front et l'API changent ensemble (le monorepo leur convient : la
CI `api-contract` protège leur contrat). Kafka, Loki ou Grafana ne changent pas
avec une PR de pricing, et servent d'autres projets. Le motif existe déjà :
`data-ingest` est un dépôt séparé raccordé par le réseau `dataplatform`.

**Écarté.**
- *Tout dans `quant-modeling`* : la plateforme aurait le cycle de vie du pricing
  et ne serait plus partageable.
- *Dans `data-ingest`* : c'est une plateforme d'ingestion, pas d'observabilité ; ce
  couplage est précisément celui qu'on évite avec la base d'audit (ADR-003).

**Coût assumé.** Deux CI, deux `git pull` sur le serveur, deux PR coordonnées
pour un changement de contrat. Acceptable : le contrat évolue rarement.

---

## ADR-003 — Postgres d'audit dédié (D3)

**Décision.** Un conteneur Postgres `qm-audit`, base `qm_audit`, séparé du
Postgres de `data-ingest`.

**Pourquoi.** Un desk sépare la source de vérité des données de marché de la piste
d'audit : la panne ou la migration de l'une ne doit pas toucher l'autre. Et la
base de marché est en lecture seule pour l'API par principe.

**Écarté.** *Un schéma dans la base de `data-ingest`* : domaines de panne
partagés, mélange de deux responsabilités.

---

## ADR-004 — Événements métier dans Kafka, télémétrie par OpenTelemetry (D4)

**Décision.** Kafka porte les événements dont on doit répondre. Métriques, logs et
traces passent par l'OTel Collector vers Prometheus, Loki et Tempo.

**Pourquoi.** La télémétrie tolère de perdre un échantillon ; la piste d'audit
non. Router la télémétrie par Kafka ajouterait une dépendance dure à un chemin
qui doit rester léger. Les deux chemins partagent `request_id` / `trace_id` pour
passer d'une ligne d'audit à sa trace.

**Écarté.** *Tout dans Kafka* : pratique à très grande échelle, disproportionné ici.

---

## ADR-005 — Au moins une fois + puits idempotent (D5)

**Décision.** Livraison *at-least-once*, `event_id` comme clé d'idempotence du
puits, commit des offsets après le commit de la base. Pas de transactions Kafka.

**Pourquoi.** Après un plantage entre le commit de la base et celui de l'offset,
le lot est relu ; l'insertion idempotente évite tout doublon. *At-least-once +
puits idempotent = exactly-once en effet*, plus simple et plus robuste que les
transactions de bout en bout, et c'est le plus courant en pratique.

**Écarté.** *Exactly-once transactionnel Kafka* : plus complexe, inutile ici.

---

## ADR-006 — Perte bornée assumée côté producteur (D6)

**Décision.** Un plantage de l'API entre la fin d'une requête et l'acquittement
de Kafka (au plus `linger.ms` = 20 ms) peut perdre des événements. Le spool local
couvre les erreurs de livraison et l'indisponibilité du broker, pas le plantage
du processus.

**Pourquoi.** Un pricing n'écrit rien : l'événement ne fait pas partie d'une
transaction métier.

**Écarté.** *Transactional outbox* (écrire l'événement en base locale dans la même
transaction que l'effet métier, puis le relayer) : bonne réponse quand
l'événement accompagne une écriture métier. **À revoir si un jour l'API écrit des
positions.**

---

## ADR-007 — JSON Schema d'abord, Avro + registre ensuite (D7)

**Décision.** Avant le lot 05, les payloads sont décrits par des JSON Schema dans
le dépôt producteur et validés par ses tests ; le sink stocke le payload en
`jsonb` sans l'interpréter. Le lot 05 introduit Avro et Apicurio Registry.

**Pourquoi.** Le contrat existe dès le début ; sa professionnalisation attend que
le reste tienne.

---

## ADR-008 — Un seul broker, sans haute disponibilité (D8)

**Décision.** Un broker KRaft, facteur de réplication 1.

**Pourquoi.** Trois brokers coûteraient ~3 Go de plus pour un besoin de
disponibilité qui n'existe pas encore. La production réelle est 3 brokers,
RF = 3, `min.insync.replicas=2` ; ce comportement s'apprend par l'exercice du lot
06, à la demande.

---

## ADR-009 — IP hachée à l'émission (D9)

**Décision.** Le producteur émet `HMAC-SHA256(secret, ip)`. L'IP en clair ne vit
que dans les logs d'accès de Loki, rétention 30 jours.

**Pourquoi.** La prod est publique : l'IP est une donnée personnelle. « En clair
30 jours puis haché » supposerait de modifier des lignes, ce qui contredit
l'append-only ; hacher à l'émission règle les deux exigences d'un coup, tout en
gardant la détection de rafales (même IP, même hachage).

---

## ADR-010 — Apache Kafka via image officielle plutôt que Confluent

**Décision.** Image `apache/kafka`, client `confluent-kafka` (librdkafka, licence
Apache 2.0). Console : **AKHQ** (Apache 2.0).

**Écarté.** *Confluent Platform* (licence communautaire, plus lourd) ; *Redpanda
Console* (BSL) ; *Kafka UI* (moins suivi).

---

## ADR-011 — Base d'audit : décisions d'implémentation du lot 02

**Décisions.**

1. **`ON CONFLICT DO NOTHING` sans cible.** Le contrat (§4) disait
   `ON CONFLICT (event_id, occurred_at) DO NOTHING`. Mesuré sur Postgres 17 : la
   forme *avec cible* exige en plus le privilège `SELECT` (pour inférer l'index),
   ce qui violerait « `audit_writer` : `INSERT` seulement ». Sans cible, `INSERT`
   suffit, et comme la clé primaire est la seule contrainte d'unicité, l'effet est
   identique. Le contrat §4 est mis à jour.
2. **Superuser d'amorçage `qm_admin`, distinct des trois rôles du contrat.**
   L'image Postgres exige un superuser au premier démarrage ; s'il s'appelait
   `audit_owner`, le rôle « migrations uniquement » contournerait tous les
   privilèges. `qm_admin` n'est joignable que par la socket locale du conteneur
   (`pg_hba` le rejette en TCP) ; les trois rôles du contrat ne sont **pas**
   superuser. Coût : un secret de plus (`AUDIT_DB_ADMIN_PASSWORD`).
3. **Déclencheur d'immuabilité.** En plus de l'absence de privilège, un déclencheur
   `BEFORE UPDATE OR DELETE` refuse la modification **même au propriétaire et à un
   superuser** (« même juste une fois » est interdit par `CLAUDE.md`). `TRUNCATE`
   n'est volontairement pas bloqué : c'est ce qui permet l'exercice de
   reconstruction depuis Kafka. Détacher/supprimer une partition n'est pas non plus
   bloqué : c'est la rétention.
4. **Partitions passées créées à l'avance** (`ensure_partitions(3, 3)`). Postgres
   refuse de créer une partition dont la plage contient déjà des lignes de la
   partition `DEFAULT` ; or une reconstruction depuis Kafka (jusqu'à 90 jours)
   réinjecte des événements des mois passés. Sans leurs partitions, ils
   atterriraient dans `DEFAULT` et bloqueraient leur création. Si le cas se produit
   malgré tout, la maintenance **avertit** (elle n'échoue pas) et
   `v_default_partition_events` montre les événements concernés.
5. **Rétention unique de 12 mois — écart assumé avec le lot 02.** Le lot demande
   12 mois pour les valorisations **et** 90 jours pour `http.access`. Or la
   rétention se fait par **partition mensuelle**, qui mélange tous les types : on
   ne peut pas retirer les `http.access` d'un mois sans `DELETE` ligne à ligne
   (interdit). Deux issues, **à trancher par le mainteneur** : (a) garder le
   compromis actuel (tout 12 mois ; `http.access` est peu volumineux ici, et Kafka
   ne le garde que 14 jours) ; (b) une table séparée `audit.http_access`, partitionnée
   elle aussi, avec sa propre rétention — c'est une modification du contrat (§3).
6. **Champs de payload lus par les vues et le contrôle qualité — hypothèses à
   confirmer par `quant-modeling`** (les payloads appartiennent au producteur, et son
   lot 18a n'est pas livré) :

   | Événement | Champs lus |
   |---|---|
   | `data.fallback` | `payload.kind` |
   | `pricing.valuation` | `payload.product`, `payload.engine.name`, `payload.model.name`, `payload.timing.duration_ms`, `payload.market_inputs[].status` (`observed`/`stale`/`proxied`/`default`) |
   | `auth.login_failed`, `auth.rate_limited` | `payload.ip_hash` (HMAC-SHA256 de l'IP) |
   | `http.access` | `payload.route`, `payload.status` |

   Un champ absent ne casse rien (les vues renvoient `NULL` ou ignorent la ligne),
   mais l'écart doit être levé par une issue croisée dans `quant-modeling`.

**Écarté.** *Créer les partitions à la demande depuis le sink* : donnerait le droit
de DDL à `audit_writer`, ce qui ruine l'INSERT seul. *Superuser = `audit_owner`* :
voir 2.

---

## ADR-012 — Télémétrie : exporteur Kafka, agent de logs, noms de métriques

**Décisions.**

1. **`kafka-exporter` (danielqsj, Apache 2.0) plutôt que JMX.** Il lit le retard par
   groupe et le débit par topic à travers l'API du broker : aucun agent JVM à ajouter
   à Kafka (donc pas de mémoire ni de port JMX en plus dans le conteneur plafonné à
   1 Go). JMX donnerait des métriques internes du broker (temps de requête, etc.) qui
   n'ont pas d'intérêt à ce volume. Le lot 06 pourra en ajouter pour la grappe.
2. **Un seul agent de logs : le Collector OTel (`filelog`), pas Grafana Alloy.** Le
   Collector est déjà là pour OTLP ; Alloy aurait été un second agent pour le même
   travail. Le nom du service vient du libellé Compose
   `com.docker.compose.service`, copié dans chaque ligne par
   `logging.options.labels` (ancre `x-logging` du compose). **Un projet qui rejoint la
   plateforme (l'API de `quant-modeling`) met la même option** ; sans elle, ses lignes
   arrivent sans `service_name`. Le Collector n'a pas accès à `docker.sock` : il lit en
   lecture seule `/var/lib/docker/containers`, ce qui l'oblige à tourner en **root** (les
   journaux Docker ne sont lisibles que par root) — compromis assumé, borné par le montage
   en lecture seule et le plafond de 128 Mo.
3. **Les logs entrent par OTLP dans Loki** (`otlphttp` vers `/otlp`), qui ne met en
   index qu'un petit ensemble d'attributs de ressource (dont `service.name`) ; tout le
   reste est de la *structured metadata*, donc jamais des étiquettes à forte
   cardinalité. Un `trace_id` dans une ligne de log JSON devient un lien vers Tempo.
4. **Garde-fou de cardinalité dans le Collector** : `request_id`, `username`, `ticker`,
   `ip`, `ip_hash`… sont **supprimés** des points de métriques avant Prometheus, quoi
   qu'envoie l'application. `scripts/check_labels.sh` vérifie Prometheus et Loki, et
   `scripts/telemetry_e2e.sh` prouve la suppression avec une métrique de test.
5. **Noms de métriques de l'API — hypothèse à confirmer par `quant-modeling`.** Le
   contrat de métriques du lot 03 ne liste que les `qm_*`. Le tableau de bord *API*
   suppose en plus les métriques HTTP de la convention sémantique OpenTelemetry stable :
   `http_server_request_duration_seconds` avec `http_route` (route **normalisée**) et
   `http_response_status_code`. Si l'auto-instrumentation FastAPI utilisée émet
   d'autres noms, on ajuste le JSON du tableau de bord, pas l'API.
6. **Rétentions** : Prometheus 15 j, Loki 30 j (l'IP en clair des logs d'accès y vit,
   ADR-009), Tempo 72 h.

**Écarté.** *Grafana Alloy* (second agent) ; *JMX exporter* (voir 1) ; *Promtail*
(en fin de vie, remplacé par Alloy).

---

## ADR-013 — Qualité des données et alertes : décisions du lot 04

**Décisions.**

1. **`data-quality` est *at-least-once* sans magasin idempotent.** Ses agrégats sont des
   compteurs en mémoire, remis à zéro au redémarrage (Prometheus gère les remises à zéro).
   Après un plantage, le lot non commité est relu et peut être compté deux fois dans la
   nouvelle incarnation : acceptable pour un signal de supervision, et c'est pourquoi rien
   d'exact (piste d'audit, facturation) ne doit jamais en dépendre. Le test de plantage
   affirme donc *aucune perte* (chaque événement traité au moins une fois, retard ramené à
   0) et **rapporte** les relectures, au lieu d'affirmer « aucun doublon » comme pour le puits.
2. **Étiquettes à ensembles fermés.** `kind` (énumération du producteur) et `status` des
   inputs sont bornés ; une valeur inconnue devient `other` / `unknown` au lieu de créer une
   série (principe 8). Toutes les séries sont **créées à 0 au démarrage** : un compteur qui
   naît à 1 n'a pas d'échantillon antérieur, `increase()` renvoie alors 0 et l'alerte ne
   partirait jamais pour le tout premier événement. **Même exigence côté producteur** pour
   `qm_audit_dropped_total` (à exporter à 0 dès le démarrage de l'API).
3. **Fenêtre d'alerte « repli » de 3 minutes** (12 scrapes de 15 s) : un événement ne peut
   pas passer entre deux évaluations, et l'alerte se résout ≈ 4 minutes après le dernier repli.
   Chaque seuil des sept règles est justifié dans `alerts/rules.yml`. Sept et non six : ajout
   de « messages en DLQ » (tout message en DLQ est un producteur qui viole le contrat).
4. **« Producteur muet » compare les deux chemins** : des pricings ont lieu (métrique OTLP
   `qm_pricing_duration_seconds_count`) mais aucune valorisation n'arrive dans Kafka depuis
   10 minutes. C'est exactement ce que la séparation des deux chemins (ADR-004) permet.
5. **Le canal de notification n'est pas choisi** (décision du mainteneur : « pas maintenant »).
   Le point de contact Grafana est un *webhook* dont l'URL vient de `ALERT_WEBHOOK_URL` ;
   tant qu'elle est vide, les alertes passent en *firing* dans Grafana mais ne notifient
   personne. `ntfy` (recommandé, auto-hébergé, notification native sur Windows) n'est pas
   construit ; le choisir ne demandera qu'un service Compose et une URL dans `.env`.
   La livraison du webhook est néanmoins **prouvée** par `scripts/alert_e2e.sh` avec un
   récepteur local jetable.
6. **Recréation des services quand une configuration change** (`scripts/config_hash.sh`).
   Découvert par le test d'alerte : `docker compose up -d` ne recrée pas un conteneur dont
   seul un fichier *monté* a changé, donc Prometheus continuait sans scruter `data-quality`.
   Le hachage des fichiers de configuration de chaque service est écrit dans `.env` puis
   posé en étiquette : un changement de configuration devient un changement de définition et
   Compose recrée uniquement ce service ; configuration inchangée = rien ne bouge.
   `deploy.sh` l'exécute ; en développement, `scripts/up.sh`.

**Écarté.** *Rechargement à chaud de Prometheus* (`--web.enable-lifecycle`) : ne couvre ni
Loki, ni Tempo, ni le Collector. *Compteurs persistés par `data-quality`* : de la complexité
pour un signal qui n'a pas besoin d'être exact.

---

## ADR-014 — Registre de schémas (lot 05)

**Décisions.**

1. **Apicurio Registry 3.1.2, mode de stockage KafkaSQL.** L'état du registre vit dans un
   topic Kafka (`kafkasql-journal`, rétention **illimitée**, déclaré dans `topics.yml`) : pas
   de base de plus. C'est une **exception assumée** au principe « Kafka n'est pas l'archive » :
   ici Kafka *est* le magasin, et perdre le journal ferait perdre tous les identifiants de
   schéma, donc la lisibilité des messages Avro déjà présents. Compensations : export
   `scripts/backup_registry.sh` (dans la même unité de sauvegarde que la base d'audit), et
   la base d'audit garde le payload en `jsonb` lisible sans registre. Apicurio crée ses topics
   internes lui-même (l'API d'administration ignore `auto.create.topics.enable`) ; ils sont
   quand même **déclarés en code** pour rester alignés par `topics.sh`.
2. **Test de fumée fait en premier, comme le lot le demande** : le client officiel
   `confluent-kafka-python` (`SchemaRegistryClient`, `AvroSerializer`) fonctionne sans
   adaptation contre la couche de compatibilité d'Apicurio ; l'alternative **Karapace**
   n'a donc pas été nécessaire.
3. **Schémas laissés au producteur** (décision demandée par le lot) : les payloads
   appartiennent à `quant-modeling` (ADR-002) ; le dépôt ne garde que des **exemples**
   (`schemas/examples/`) pour ses tests et exercices. Le registre est l'endroit où les deux
   dépôts se rencontrent.
4. **Événement entier en Avro, pas seulement le payload.** Un seul schéma par topic, un seul
   identifiant par message ; le contrat d'enveloppe reste vérifié après décodage par le même
   JSON Schema que pour le JSON.
5. **Client de registre minimal dans `qp_common`** (`GET /schemas/ids/{id}`, mis en cache
   indéfiniment car un identifiant est immuable) plutôt que la bibliothèque officielle :
   le sink n'a besoin que de *lire* un schéma par identifiant, et évite ainsi `httpx`,
   `authlib` et `cachetools` dans une image plafonnée à 128 Mo. La bibliothèque officielle
   sert aux tests.
6. **Pas d'authentification sur le registre** : joignable seulement sur `dataplatform` et
   `127.0.0.1`. Un producteur compromis pourrait enregistrer un schéma compatible, mais pas
   en briser un existant (`BACKWARD`). À revoir avec SASL/ACL (lot 06).

**Écarté.** *Confluent Schema Registry* (licence communautaire, ADR-010) ; *registre sur
Postgres* (un second rôle, une seconde base à sauvegarder) ; *Avro seulement pour le payload*
(deux schémas par message, enveloppe non versionnée).
