# Exercices

Deux parties :

- **Partie A — exercices guidés** des lots 01 à 05, à faire **sur la plateforme de
  développement** (`~/quant-platform`, pile lancée par `scripts/up.sh`). Ce sont des
  consignes : les commandes sont prêtes à copier, les réponses attendues sont indiquées,
  et **c'est à toi de les faire** (ils n'ont pas été rejoués à ta place).
- **Partie B — exercices d'opérateur** du lot 06, faits dans un **laboratoire jetable**,
  avec les sorties **réellement observées** collées telles quelles.

Raccourci utilisé partout :

```bash
K() { docker compose exec -T kafka env KAFKA_HEAP_OPTS="-Xmx64m" /opt/kafka/bin/"$1" --bootstrap-server localhost:9092 "${@:2}"; }
```

> ⚠️ La base d'audit est **append-only** : chaque ligne insérée par un exercice y reste. Ces
> exercices ne doivent pas être faits sur la pile de production (`~/quant-platform-prod`).
> `docker compose down -v` remet la pile de développement à zéro.

---

# Partie A — Exercices guidés

## Lot 01 — Kafka

**A1.1 — La clé décide de la partition.** Le producteur hache la clé pour choisir la partition ;
l'ordre n'est garanti qu'*à l'intérieur* d'une partition.

```bash
# 3 partitions, comme qm.audit.valuation.v1 (on crée un topic de test dans le lab, pas ici :
# voir la partie B pour un environnement où l'on peut créer n'importe quoi)
scripts/lab_up.sh
LAB="docker compose -f docker-compose.lab.yml"
$LAB exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic cles --partitions 3 --replication-factor 3
printf 'alice|1\nbob|2\nalice|3\ncarol|4\nbob|5\nalice|6\n' | $LAB exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic cles --property parse.key=true --property key.separator='|'
$LAB exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic cles --from-beginning --timeout-ms 5000 --property print.key=true --property print.partition=true
```

*Question :* les trois messages d'`alice` sont-ils dans la même partition ? Dans quel ordre
sortent-ils ? Que deviendrait cet ordre si la clé était `request_id` (différent à chaque message) ?

**A1.2 — Offsets et groupes.** Un *offset* est la position d'un consommateur dans une partition ;
deux **groupes** ont chacun leurs offsets et ne se volent pas les messages.

```bash
K kafka-console-consumer.sh --topic qm.audit.valuation.v1 --group exercice-a --from-beginning --max-messages 3 --timeout-ms 8000
K kafka-console-consumer.sh --topic qm.audit.valuation.v1 --group exercice-a --max-messages 3 --timeout-ms 8000    # reprend APRÈS les 3 premiers
K kafka-console-consumer.sh --topic qm.audit.valuation.v1 --group exercice-b --from-beginning --max-messages 3 --timeout-ms 8000   # autre groupe : relit les 3 premiers
K kafka-consumer-groups.sh --describe --group exercice-a
```

*Question :* pourquoi le deuxième groupe revoit-il les mêmes messages, et pourquoi n'est-ce pas un
problème (c'est ce que fait `data-quality` par rapport à `audit-sink`) ?

**A1.3 — Que reste-t-il après un arrêt ?**

```bash
docker compose restart kafka           # les messages sont-ils toujours là ? (volume nommé)
K kafka-get-offsets.sh --topic qm.audit.valuation.v1 --time -1
# ⚠ seulement sur une pile jetable :
docker compose down -v && scripts/up.sh   # que reste-t-il ? Qu'est-ce que ça dit de « Kafka n'est pas l'archive » ?
```

## Lot 02 — Base d'audit et puits

**A2.1 — Deux instances du sink dans le même groupe.** Les services de la plateforme ont un
`container_name` fixe (pour être joignables par leur nom sur `dataplatform`), donc Compose refuse
de les dupliquer. Pour l'exercice, crée un fichier **non commité** `docker-compose.override.yml`
qui déclare une seconde instance :

```yaml
services:
  audit-sink-2:
    extends: {file: docker-compose.yml, service: audit-sink}
    container_name: audit-sink-2
    environment: {SINK_METRICS_PORT: "9118"}
```

`docker compose up -d audit-sink-2`, puis *AKHQ* (`http://127.0.0.1:8181` → *Consumer Groups* →
`audit-sink`) : les 11 partitions se répartissent entre les deux instances. Tue l'une
(`docker kill audit-sink-2`) et regarde la réaffectation. Supprime l'override ensuite.
La partie B (exercice 2) mesure ce rééquilibrage.

**A2.2 — Un second groupe, qui n'écrit nulle part.** C'est le principe qu'exploite `data-quality`.

```bash
K kafka-console-consumer.sh --topic qm.audit.valuation.v1 --group curieux --from-beginning --timeout-ms 6000 | wc -l
scripts/dq_independence_test.sh   # la version automatisée : arrêter l'un des deux groupes n'affecte pas l'autre
```

**A2.3 — Retarder le commit après l'insertion, et tuer le sink.** Le cœur de l'*at-least-once*.

```bash
scripts/crash_test.sh    # relance le sink avec SINK_DEBUG_DELAY_MS=150 puis kill -9
docker compose logs audit-sink | grep '"duplicates": [1-9]'   # les lots relus après le plantage : insérés 0 fois, ignorés N fois
```

*Question :* où serait le doublon si l'insertion n'était pas idempotente ? Que faudrait-il changer
pour que le sink commit **avant** l'insertion, et quel serait le risque (perte au lieu de doublon) ?

## Lot 03 — Télémétrie

**A3.1 — Une explosion de cardinalité.** Voir pourquoi la règle « jamais un `request_id` comme
étiquette » existe.

1. Dans `otel/collector.yml`, mettre en commentaire la suppression de `request_id` (processeur
   `transform/forbidden_metric_labels`), puis `scripts/up.sh` (recrée seulement le Collector).
2. Envoyer 5 000 points avec un `request_id` distinct :

```bash
for i in $(seq 1 5000); do
  curl -s -X POST http://127.0.0.1:4318/v1/metrics -H 'Content-Type: application/json' -d "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"cardinalite\"}}]},\"scopeMetrics\":[{\"metrics\":[{\"name\":\"qm_test_total\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[{\"asInt\":\"1\",\"timeUnixNano\":\"$(date +%s%N)\",\"attributes\":[{\"key\":\"request_id\",\"value\":{\"stringValue\":\"req_$i\"}}]}]}}]}]}]}" >/dev/null
done
curl -s 'http://127.0.0.1:9091/api/v1/query?query=count(qm_test_total)'   # 5000 séries pour UNE métrique
scripts/check_labels.sh                                                   # doit maintenant échouer, en nommant request_id
docker stats --no-stream prometheus                                        # la mémoire monte
```

3. Remettre la suppression en place, `scripts/up.sh`. *Question :* combien de séries donnerait un
   ticker (500 valeurs) × un utilisateur (50) × une route (20) ?

**A3.2 — De la métrique à la trace, puis à la ligne d'audit.** Une requête lente : dans le
tableau de bord *Pricing*, repérer le p95 le plus haut, ouvrir le tableau *Slowest valuations*,
cliquer sur `trace_id` (ouvre Tempo), puis dans Loki chercher `{service_name="…"} |= "<trace_id>"`.
(Nécessite que l'API de `quant-modeling` émette ses événements et traces — lots 18a–18d.)

## Lot 04 — Qualité des données et alertes

**A4.1 — Trois instances de `data-quality` dans le même groupe.** Comme A2.1 (un
`docker-compose.override.yml` local, `extends` du service `data-quality`, port de métriques
différent pour chaque instance). Observer le rééquilibrage dans AKHQ quand on en tue une.

**A4.2 — Le même taux de repli, par deux chemins.** Sur une fenêtre de 24 h :

```bash
docker compose exec -T qm-audit psql -U qm_admin -d qm_audit -c "SELECT kind, events FROM audit.v_fallbacks_daily WHERE day = current_date"
curl -sG 'http://127.0.0.1:9091/api/v1/query' --data-urlencode 'query=sum by (kind) (increase(qp_dq_fallbacks_total[24h]))'
```

*Question :* pourquoi les deux nombres peuvent-ils légèrement différer (redémarrage de
`data-quality`, relecture après plantage, fenêtre `increase()` extrapolée) ? Lequel fait foi
(ADR-004, ADR-013 §1) ?

## Lot 05 — Registre de schémas

**A5.1 — Casser volontairement la compatibilité.** Éditer une copie de
`schemas/examples/valuation.v1.avsc` en ajoutant `{"name":"model_name","type":"string"}` **sans
défaut** au payload, puis tenter de l'enregistrer :

```bash
curl -s -X POST http://127.0.0.1:8082/apis/ccompat/v7/subjects/exercice-value/versions -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d "{\"schema\": $(python3 -c 'import json,sys; print(json.dumps(open("schemas/examples/valuation.v1.avsc").read()))')}"
# puis la version cassée : le registre répond 409 et nomme le champ. Corrige en ajoutant "default": "" et recommence.
```

Ou lancer `.venv/bin/python scripts/registry_test.py` et lire l'étape 4.

**A5.2 — Taille JSON contre Avro.** L'étape 5 de `scripts/registry_test.py` compare un même
événement ; sur ce jeu d'exemple l'Avro fait ≈ 53 % du JSON. Refaire la mesure avec un événement
plus riche (10 `market_inputs`) : l'écart se creuse-t-il ou se réduit-il, et pourquoi (les noms de
champs ne sont plus répétés dans chaque message) ?


---

# Partie B — Exercices d'opérateur (lot 06)

Tout se passe dans le **laboratoire** (`docker-compose.lab.yml`, projet `quant-platform-lab`) :

```bash
scripts/lab_up.sh [replay|sasl|solo …]   # vérifie l'isolation, puis démarre la grappe de 3 brokers
scripts/lab_down.sh                      # détruit tout et vérifie qu'il ne reste rien
```

Les sorties ci-dessous sont **celles qui ont été obtenues** le 2026-09-20 (Kafka 4.1.1, 3 brokers
KRaft en mode combiné broker+contrôleur, `default.replication.factor=3`, `min.insync.replicas=2`),
collées telles quelles. Quand une mesure a une limite, elle est dite. Un premier essai de
l'exercice 2 a d'ailleurs été **jeté** : il mesurait la chronologie de tout le groupe, qui masque
un rééquilibrage (les partitions non touchées continuent de couler) ; il faut mesurer *par partition*.

## Exercice 1 — Grappe de 3 brokers : ce que RF, ISR et `min.insync.replicas` garantissent

Topic `ex1` : 3 partitions, RF 3, `min.insync.replicas=2`. Production avec `acks=all`.

```text
### 1. baseline (3 brokers up): produce 100
  producer errors: 0
  messages in topic: 100

### 2. stop kafka-3 (1 of 3 down): produce 100 more
	Topic: ex1	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
	Topic: ex1	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2
	Topic: ex1	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,1
  producer errors: 0
  messages in topic: 200

### 3. stop kafka-2 too (2 of 3 down): produce 100 more
  producer errors: 100
  messages in topic (as far as kafka-1 can tell): ex1:1:78 ex1:2:68 

### 4. restart kafka-2 and kafka-3
	Topic: ex1	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2
	Topic: ex1	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2
	Topic: ex1	Partition: 2	Leader: 1	Replicas: 2,3,1	Isr: 1,2
  messages in topic after recovery: 200  (200 acknowledged before; the 100 refused were NOT written)

### 5. produce 100 more, all 3 back
  messages in topic: 300

### 6. ISR once everything has caught up
	Topic: ex1	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2,3
	Topic: ex1	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: ex1	Partition: 2	Leader: 1	Replicas: 2,3,1	Isr: 1,2,3
```

**Ce qu'on en retient.**

- **Un broker en moins : rien ne se voit côté écriture.** L'ISR de chaque partition passe de
  `3,1,2` à deux membres, les 100 envois suivants réussissent (200 messages). C'est ce que
  paie le RF = 3.
- **Deux brokers en moins : l'écriture s'arrête**, et *bruyamment* : 100 envois sur 100 en
  erreur, aucun écrit (le total reste à 200 à la reprise, puis 300 après les 100 suivants). Avec
  `acks=all`, Kafka préfère **refuser** une écriture qu'il ne peut pas répliquer sur 2 copies :
  c'est le choix cohérence contre disponibilité. Un refus est récupérable (le producteur de
  l'API retombe sur son spool) ; une perte silencieuse ne l'est pas.
- **Deux causes se superposent dans cette grappe** : `min.insync.replicas=2` (il ne reste qu'un
  réplica) *et* la perte du quorum des contrôleurs (les nœuds sont aussi contrôleurs : 1 sur 3
  ne suffit pas, il en faut 2). C'est pourquoi `kafka-get-offsets` n'a pu répondre que pour deux
  partitions sur trois. En production on sépare souvent brokers et contrôleurs.
- **Le rattrapage prend quelques secondes** : la première lecture après le redémarrage montrait
  encore `Isr: 1,2` pour `kafka-3`, la suivante `Isr: 1,2,3`.
- **Les leaders ne se redistribuent pas d'eux-mêmes tout de suite** : après la reprise les trois
  partitions ont `Leader: 1`. Le rééquilibrage du leadership (élection du réplica préféré,
  `kafka-leader-election.sh --election-type preferred`) est une opération distincte du rattrapage.

## Exercice 2 — Rééquilibrage d'un groupe de consommateurs

Trois consommateurs (`lab/probe.py`, image du sink) dans un groupe sur `ex2` (6 partitions), un
producteur à 50 messages/s sur 97 clés. On retire le consommateur `c3` et on mesure, **partition par
partition**, le plus long silence entre deux messages qui encadre l'instant du retrait
(`scripts/lab_rebalance.sh STRATÉGIE MODE`). `kill` = `kill -9` (le broker doit s'en apercevoir),
`stop` = `SIGTERM` (le consommateur quitte le groupe proprement).

```text
=== stratégie / mode: range kill
after the kill of c3 (t0 = the moment of the kill):
normal gap between messages of one partition (median): 101 ms
partitions owned by the killed consumer: longest stall 10.6 s   [p2=10.6s, p3=10.4s]
partitions owned by the survivors: longest stall 0.4 s   [p0=0.4s, p1=0.3s, p4=0.4s, p5=0.4s]
=== stratégie / mode: cooperative-sticky kill
after the kill of c3 (t0 = the moment of the kill):
normal gap between messages of one partition (median): 81 ms
partitions owned by the killed consumer: longest stall 10.8 s   [p0=10.3s, p2=10.8s]
partitions owned by the survivors: longest stall 0.4 s   [p1=0.3s, p3=0.3s, p4=0.4s, p5=0.4s]
=== stratégie / mode: consumer kill
after the kill of c3 (t0 = the moment of the kill):
normal gap between messages of one partition (median): 60 ms
partitions owned by the killed consumer: longest stall 46.6 s   [p2=46.6s, p5=43.7s]
partitions owned by the survivors: longest stall 0.4 s   [p0=0.4s, p1=0.3s, p3=0.3s, p4=0.4s]
=== stratégie / mode: range stop
after the stop of c3 (t0 = the moment of the stop):
normal gap between messages of one partition (median): 101 ms
partitions owned by the killed consumer: longest stall 1.6 s   [p2=1.6s, p3=1.4s]
partitions owned by the survivors: longest stall 0.4 s   [p0=0.4s, p1=0.3s, p4=0.4s, p5=0.4s]
=== stratégie / mode: cooperative-sticky stop
after the stop of c3 (t0 = the moment of the stop):
normal gap between messages of one partition (median): 101 ms
partitions owned by the killed consumer: longest stall 1.7 s   [p0=1.4s, p2=1.7s]
partitions owned by the survivors: longest stall 0.4 s   [p1=0.3s, p3=0.3s, p4=0.4s, p5=0.4s]
=== stratégie / mode: consumer stop
after the stop of c3 (t0 = the moment of the stop):
normal gap between messages of one partition (median): 101 ms
partitions owned by the killed consumer: longest stall 3.9 s   [p2=1.7s, p5=3.9s]
partitions owned by the survivors: longest stall 0.4 s   [p0=0.4s, p1=0.3s, p3=0.3s, p4=0.4s]
done=1
```

**Lecture, avec ses limites.**

| Retrait de `c3` | partitions de `c3` : silence | partitions des survivants |
|---|---|---|
| `range` (classique), **kill -9** | **10,4 – 10,6 s** | 0,3 – 0,4 s |
| `cooperative-sticky`, **kill -9** | **10,3 – 10,8 s** | 0,3 – 0,4 s |
| protocole `consumer` (KIP-848), **kill -9** | **43,7 – 46,6 s** | 0,3 – 0,4 s |
| `range`, arrêt propre | 1,4 – 1,6 s | 0,3 – 0,4 s |
| `cooperative-sticky`, arrêt propre | 1,4 – 1,7 s | 0,3 – 0,4 s |
| protocole `consumer`, arrêt propre | 1,7 – 3,9 s | 0,3 – 0,4 s |

- **Le coût dominant est la *détection* de la mort, pas le rééquilibrage.** Un consommateur tué
  sans prévenir laisse ses partitions muettes pendant tout le `session.timeout.ms` : ≈ 10 s en
  classique (le réglage de la sonde), ≈ 45 s avec le protocole `consumer` (son délai par défaut).
  Un départ propre les rend en 1 à 4 s. **Conséquence pour le sink** : il doit traiter `SIGTERM`
  (il le fait : « finir le lot, commiter, quitter », et `consumer.close()` quitte le groupe) — un
  `docker stop` propre coûte ~1,5 s de retard, un `kill -9` ~10 s.
- **Ce que cette mesure ne montre pas** : `range` (rééquilibrage « stop the world ») et
  `cooperative-sticky` (incrémental) donnent **le même résultat** ici. Avec 6 partitions, aucun
  état à reconstruire et un traitement instantané, un rééquilibrage complet ne coûte que
  ≈ 0,3 s à tout le monde. L'avantage du coopératif apparaît quand il y a beaucoup de partitions
  ou qu'un consommateur garde un état coûteux à recharger à chaque réaffectation — ce que ce
  banc ne reproduit pas. **On n'a donc pas mesuré cet avantage ; on ne le déduit pas.**
- Mesure sur **une passe** par configuration : les ordres de grandeur (10 s / 45 s / ~1,5 s) sont
  robustes, les dixièmes de seconde ne le sont pas.

## Exercice 3 — SASL/SCRAM et ACL

Un broker à part (`kafka-sasl`) avec deux portes : `ADMIN` en clair, **à l'intérieur du conteneur
seulement** (pour créer utilisateurs et ACL), et `CLIENT` en SASL/SCRAM sur `:9095`. Identités :
`qm-api` (produit sur `qm.*`) et `qm-sink` (consomme `qm.*` dans le groupe `audit-sink`).
`scripts/lab_sasl.sh` déroule tout :

```text
  ✓ qm-api PRODUCES on qm.audit.valuation.v1
  ✓ qm-api CANNOT consume qm.audit.valuation.v1
  ✓ qm-api CANNOT produce outside qm.* (other.topic.v1)
  ✓ qm-sink CONSUMES qm.audit.valuation.v1 as group audit-sink
  ✓ qm-sink CANNOT produce on the audit topics
  ✓ qm-sink CAN write the DLQ (its own rejected messages)
  ✓ qm-sink CANNOT consume with another group
  ✓ a wrong password is refused
  ✓ an unknown user is refused
  ✓ no credentials at all is refused
```

Les refus sont de **vraies erreurs d'autorisation**, pas des délais dépassés (relevé séparé) :

```text
--- qm-api tente de CONSOMMER
org.apache.kafka.common.errors.GroupAuthorizationException: Not authorized to access group: g-api
--- qm-api tente de PRODUIRE hors qm.*
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [other.topic.v1]
--- qm-sink tente de PRODUIRE
org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.
org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.
--- mauvais mot de passe
[2026-09-20 11:50:52,724] ERROR [Producer clientId=console-producer] Connection to node -1 (kafka-sasl/172.24.0.5:9095) failed authentication due to: Authentication failed during authentication due to invalid credentials
org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256
```

**Ce qu'on en retient.**

- **Deux couches distinctes** : l'*authentification* (SCRAM : qui es-tu ? mauvais mot de passe →
  `SaslAuthenticationException`) et l'*autorisation* (ACL : que peux-tu faire ? →
  `TopicAuthorizationException`, `GroupAuthorizationException`). Les ACL se posent par *préfixe*
  (`qm.`) : un nouveau topic `qm.…` est couvert d'office.
- **Trouvaille pour la vraie plateforme : « le sink peut seulement consommer » est faux.** Le sink
  *produit* aussi : chaque message rejeté part dans `qm.dlq.v1`. Sans droit d'écriture il tombe
  en `ClusterAuthorizationException` à son premier message empoisonné. Le droit minimal correct :
  `Read` sur `qm.` + `Read` sur le groupe `audit-sink` + **`Write` sur `qm.dlq.v1` seulement**
  (ce dernier point a été vérifié : `Write` sur le topic suffit à un producteur idempotent, pas
  besoin du droit `IdempotentWrite` sur le cluster).
  Même chose pour `data-quality` : lecture seule, mais son propre groupe (`data-quality`).
- **Limite de ce montage** : `SASL_PLAINTEXT` protège le mot de passe (échange SCRAM) mais **pas les
  données**, qui circulent en clair. En production : `SASL_SSL`. Et le super-utilisateur anonyme
  de la porte `ADMIN` n'a de sens que dans un conteneur jetable.

## Exercice 4 — Compaction

Topic `ex4` en `cleanup.policy=compact` (réglé pour agir en quelques secondes : `segment.ms=2000`,
`min.cleanable.dirty.ratio=0.01`). Trois clés A, B, C mises à jour 10 fois chacune :

```text
### topic with cleanup.policy=compact (fast settings so it happens in a minute)
Created topic ex4.
### produce 10 updates for each of the keys A, B, C (30 records)
### BEFORE compaction: every update is there
  records: 30
  Offset:0	A	A-v1
  Offset:1	B	B-v1
  Offset:2	C	C-v1
  Offset:3	A	A-v2
  Offset:4	B	B-v2
  Offset:5	C	C-v2
### roll the segment (compaction only cleans CLOSED segments) and wait for the cleaner
### AFTER compaction (6 s later): only the latest value of each key
  records: 4
  Offset:27	A	A-v10
  Offset:28	B	B-v10
  Offset:29	C	C-v10
  Offset:30	D	D-v1
### a tombstone (key with an empty value) deletes the key once compacted

### after the tombstone for A: roll a segment and let the cleaner run again
  (waited 6 s)
  Offset:28	B	B-v10
  Offset:29	C	C-v10
  Offset:30	D	D-v1
  Offset:31	A	null
  Offset:32	E	E-v1
```

**Ce qu'on en retient.**

- La compaction garde **la dernière valeur de chaque clé** : 30 enregistrements → 4. C'est un
  bon modèle pour un *état courant* (dernier prix, dernière config), mauvais pour un *journal*.
- Les **offsets gardent des trous** (27, 28, 29, 30…) : les enregistrements supprimés ne sont pas
  renumérotés.
- **Seuls les segments fermés sont compactés** : il a fallu écrire un enregistrement de plus,
  après `segment.ms`, pour que le segment actif se ferme.
- Une **pierre tombale** (clé avec valeur vide) : l'ancienne valeur de `A` (offset 27) a disparu
  et la pierre tombale elle-même (offset 31, `null`) reste un moment, le temps que les lecteurs
  la voient, avant d'être supprimée à son tour (`delete.retention.ms`).
- **Pourquoi ce n'est pas adapté à la piste d'audit** : chaque valorisation est un événement
  *distinct* qu'on doit pouvoir rejouer ; la compaction en ferait disparaître l'historique dès que
  deux événements partagent une clé (ici `username`) — exactement ce qu'on ne veut jamais.

## Exercice 5 — Rejeu à grande échelle

1 000 000 d'événements dans `qm.audit.valuation.v1` (3 partitions, RF 3), puis pour chaque taille de
lot : table vidée, groupe rembobiné au début, chronométrage du sink (une instance, 2 cœurs
plafonnés) jusqu'à 1 000 000 de lignes (`scripts/lab_replay.sh`). Production : 30 s.

```text
✓ lab isolation verified: own project, no shared network, no published port
✓ 3-broker cluster is up
→ database and topic
→ producing 1000000 events (unique UUID v7 each; the sink dedups on event_id)
  (le producteur console a affiché des avertissements « REQUEST_TIMED_OUT … retrying », omis ici)
  produced in 30 s
  topic holds 1002827 messages

batch      seconds    events/s     | CPU% (mean while replaying)
                                   | sink       postgres   broker(avg)
50         598        1672         | 65         24         5         
500        243        4115         | 70         42         2         
5000       180        5555         | 92         30         2         
(CPU% is per container, 100 = one core; the sink and Postgres are capped at 2 cores, each broker at 1.)

rows in the lab database: 1000000  distinct event_id: 1000000
messages in the topic: 1002827
duplicates skipped by the sink during the last (5000) run: 2827
```

**Ce qu'on en retient.**

- **Le débit dépend fortement de la taille de lot** : ×2,5 de 50 à 500, puis +35 % de 500 à 5 000. La
  courbe s'aplatit : un petit lot paie surtout des allers-retours (commit Postgres, commit d'offset
  synchrone) ; un grand lot amortit ces coûts et il reste le coût *par message*.
- **Le goulot est le sink, pas Kafka ni Postgres.** Pendant le rejeu, le sink consomme
  65–92 % d'un cœur (Python : validation JSON Schema et adaptation des paramètres, un seul fil),
  Postgres 24–42 %, un broker 2–5 %. Au plus haut débit (≈ 5 500 événements/s) le sink est
  proche de la saturation d'un cœur, soit ≈ 180 µs de CPU par événement. Pistes, non essayées :
  plusieurs instances (les 3 partitions autorisent 3 instances en parallèle), un lot plus grand
  n'aidera guère au-delà.
- **Ordre de grandeur utile** : à 5 500/s, 90 jours de Kafka au rythme réel de la plateforme (quelques
  événements par seconde, soit quelques millions au pire) se rejouent en quelques minutes.
  Le lot par défaut (500) est un bon compromis pour le régime normal ; on ne le change pas.
- **Bonus non prévu, et instructif** : le producteur console a subi des `REQUEST_TIMED_OUT` et a
  réessayé, d'où **1 002 827 messages pour 1 000 000 d'événements uniques**. Le sink en a ignoré
  exactement 2 827 (`duplicates`) et la table compte 1 000 000 de lignes distinctes : l'idempotence
  n'est pas qu'un test de plantage, elle absorbe aussi les doublons du producteur.
- **Limite** : une seule passe par taille de lot, sur une machine partagée ; les débits absolus
  valent pour ce banc, pas pour la production.

## Exercice 6 — Perte de disque : grappe de 3 contre broker unique

Même accident (le volume d'un broker est détruit, puis le broker redémarre) dans deux architectures
(`scripts/lab_disk_loss.sh`) :

```text
### A. 3 brokers, RF=3, min.insync.replicas=2
  messages before the accident: 300
  kafka-3 stopped and its volume DELETED (a dead disk)
  after restarting kafka-3 on an EMPTY disk:
    	Topic: ex6	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
    	Topic: ex6	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,1,3
    	Topic: ex6	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1,2,3
  messages after: 300   (kafka-3 re-copied its replicas from the other two)

### B. ONE broker, RF=1 (what the platform runs today)
  messages before the accident: 300
  kafka-solo stopped and its volume DELETED
  topics after restarting on an EMPTY disk: [ ]
  (nothing to count: the topic, its messages and every consumer offset are gone)

data on kafka-3's NEW disk, per partition of ex6:
  ex6-0: 16 KiB, log segment 1507 bytes
  ex6-1: 16 KiB, log segment 1447 bytes
  ex6-2: 16 KiB, log segment 1621 bytes
```

**Ce qu'on en retient.** C'est la justification concrète de la haute disponibilité (ADR-008) :

- **Grappe RF = 3** : le broker reparti sur un disque vide **recopie ses réplicas depuis les deux
  autres** (les segments sont revenus sur son nouveau disque), l'ISR redevient complet, aucun
  message n'est perdu, personne n'a eu à intervenir.
- **Broker unique RF = 1 — la plateforme aujourd'hui** : le disque perdu, c'est **le topic, ses
  messages et tous les offsets des groupes** qui disparaissent (`topics: [ ]`). Conséquence
  précise : ce qui n'avait pas encore été lu par le sink est perdu (ADR-006, « Kafka n'est pas
  l'archive »), mais **ce qui est déjà en base d'audit reste**, et la sauvegarde `pg_dump` couvre
  le reste. C'est pourquoi la source de vérité durable est Postgres et que le sink lit vite.
  Après une telle perte, il faut aussi remettre en place les topics (`docker compose run --rm
  topics-init`) ; l'offset du groupe est perdu, le sink repart du plus ancien disponible.

## Critères d'acceptation du lot

- Le lab se démarre et se détruit avec deux commandes (`scripts/lab_up.sh`, `scripts/lab_down.sh`),
  sans laisser de conteneur, de volume ni de réseau (vérifié par `lab_down.sh` lui-même).
- La plateforme n'a pas été touchée : ses conteneurs n'ont pas redémarré pendant le lot
  (`docker inspect -f '{{.State.StartedAt}}'` comparé avant/après — voir la fin de ce fichier).

### Vérification faite à la fin du lot

```text
$ scripts/lab_down.sh
 Network quant-platform-lab_default Removed
 Volume quant-platform-lab_lab-audit-data Removed
✓ lab destroyed: no container, volume or network left

conteneurs / volumes / réseaux restants du projet quant-platform-lab :  0 / 0 / 0

$ diff <(démarrages des conteneurs de la pile avant le lot) <(après)
IDENTIQUES : aucun conteneur de la plateforme n'a redémarré
```
