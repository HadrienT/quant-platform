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
