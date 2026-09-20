# Runbook — quant-platform

Exploitation de la plateforme (Kafka, base d'audit, télémétrie) sur le serveur.
Ce document s'étoffe lot après lot : chaque section indique le lot qui la rend
vraie. Quelqu'un qui n'a jamais vu le dépôt doit pouvoir démarrer la plateforme
**à froid** en suivant §2.

## 1. Le modèle : deux dossiers, un seul dépôt

| Dossier | Rôle | Ce qu'on y fait |
|---|---|---|
| `~/quant-platform` | **Développement** : branches, commits, PR. Les sessions Claude travaillent ici. | tout le dev |
| `~/quant-platform-prod` | **La plateforme en ligne.** Toujours sur `main`. | `./scripts/deploy.sh`, et rien d'autre |

Les deux dossiers partagent **le même dépôt git** (mécanisme `git worktree`) : un
worktree est un second dossier de travail attaché au même historique, chacun sur
une branche différente. Le dossier de prod reste sur `main` et ne sert qu'à
déployer ; on n'y édite ni n'y commite jamais. Une même branche ne pouvant être
extraite que dans un seul dossier, le dossier de développement travaille sur des
branches de lot (`wp/NN-…`), jamais sur `main`.

**Redéployer l'API ne redémarre pas la plateforme, et inversement.** Un merge dans
`main` ne change rien en ligne tant que `deploy.sh` n'est pas lancé.

```bash
git worktree list          # doit montrer les deux dossiers
```

## 2. Démarrage à froid

Prérequis : Docker + Compose v2, `git`, `openssl`.

```bash
git clone <url-du-dépôt> ~/quant-platform          # dev (ou le dépôt existe déjà)
cd ~/quant-platform
git worktree add ~/quant-platform-prod main         # crée le dossier de prod, sur main
cd ~/quant-platform-prod
./scripts/init_env.sh                               # crée .env avec des secrets aléatoires
./scripts/deploy.sh                                 # réseau, build, up, attend « healthy »
```

- `init_env.sh` refuse d'écraser un `.env` existant. **`.env` n'est nulle part dans
  git** : garder une copie hors du dépôt (`cp .env ~/quant-platform-env.bak`). Le
  perdre n'est pas fatal pour les données (elles sont dans les volumes) mais les
  mots de passe de la base d'audit ne se retrouvent pas.
- `deploy.sh` crée le réseau externe `dataplatform` s'il manque, puis
  `docker compose up -d --wait` : il ne rend la main que lorsque chaque service est
  sain et chaque tâche ponctuelle (création des topics, migrations) terminée.
- Relancer `deploy.sh` est sans danger : sans changement, rien ne bouge.

## 3. Déployer un changement

```bash
cd ~/quant-platform          # dev : branche wp/…, commits, PR, merge dans main
cd ~/quant-platform-prod && ./scripts/deploy.sh
```

`deploy.sh` fait `git pull --ff-only` s'il existe un dépôt distant joignable (non
fatal hors ligne), puis reconstruit et relance. Tant qu'aucun dépôt distant n'est
configuré, la mise à jour de `main` se fait à la main depuis le dossier de prod :
`git merge --ff-only wp/NN-…` (avance `main` sans créer de commit de fusion ; refuse
de s'exécuter si ce n'est pas un simple avancement, donc réversible et sans risque).

## 4. Démarrage au boot (systemd)

```bash
sudo cp deploy/quant-platform.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now quant-platform.service
```

L'unité impose l'**ordre** : la plateforme démarre **avant** l'API
(`Before=quant-modeling.service`), mais l'API ne *dépend* pas d'elle : si la
plateforme échoue ou traîne (au plus 5 minutes), l'API démarre quand même et
utilise son spool local. Rien ici ne doit devenir un point de défaillance du site.

## 5. Base d'audit : sauvegarde, restauration, partitions (lot 02)

**Sauvegarde.** `scripts/backup_audit.sh [DEST]` fait un `pg_dump` (format custom)
vers `~/backups/quant-platform` (ou `$BACKUP_DEST`), vérifie que le fichier est
lisible avant d'élaguer, et garde les 14 derniers. Le timer systemd
`deploy/quant-platform-backup.timer` le lance chaque nuit à 03:15 (installation :
en-tête de `deploy/quant-platform-backup.service`). **Mettre `BACKUP_DEST` hors de la
machine** (NAS, disque USB, cible rclone) : une sauvegarde sur le même disque ne
protège pas d'une perte de disque.

**Vérifier une sauvegarde** (la seule preuve qu'elle sert) :

```bash
scripts/restore_audit.sh ~/backups/quant-platform/audit-<date>.dump   # → base qm_audit_restore
docker compose exec qm-audit psql -U qm_admin -d qm_audit_restore -c 'select count(*) from audit.events'
docker compose exec qm-audit psql -U qm_admin -d postgres -c 'drop database qm_audit_restore'
```

Le script **refuse** de restaurer sur `qm_audit` (la base vivante).

**Remplacer la base vivante après un sinistre** (procédure manuelle, à froid) :
`docker compose stop audit-sink` → restaurer dans `qm_audit_restore` comme ci-dessus →
`docker compose exec qm-audit psql -U qm_admin -d postgres -c 'ALTER DATABASE qm_audit
RENAME TO qm_audit_old' -c 'ALTER DATABASE qm_audit_restore RENAME TO qm_audit'` →
`docker compose up -d audit-sink`. Les événements postérieurs à la sauvegarde sont
rejoués depuis Kafka si l'offset du groupe est resté en arrière (`kafka-consumer-groups
--reset-offsets`, voir `scripts/audit_e2e.sh` scénario 5) et dans la limite de la
rétention Kafka (90 jours) ; l'insertion étant idempotente, rejouer trop loin est sans danger.

**Migrations.** `docker compose run --rm audit-migrate` applique `migrations/*.sql`
dans l'ordre (à chaque `up`). Chaque fichier est enregistré avec son `sha256` ; **modifier
une migration déjà appliquée est refusé** : ajouter un nouveau fichier.

**Partitions.** `audit.events` est partitionnée par mois. La maintenance
(`docker compose run --rm audit-migrate maintain`, timer
`quant-platform-maintenance.timer` à 02:40) crée de 3 mois en arrière à 3 mois en avant, et
**détache puis supprime** les partitions entièrement au-delà de `AUDIT_RETENTION_MONTHS`
(12 par défaut). Aperçu sans rien supprimer : `AUDIT_DRY_RUN=1 docker compose run --rm
audit-migrate maintain`. Limite connue : la rétention porte sur des mois entiers, tous types
confondus (ADR-011 §5).

Si un avertissement dit qu'une partition ne peut pas être créée parce que `DEFAULT` contient
déjà ses événements : `SELECT * FROM audit.v_default_partition_events`. Un événement daté
d'un mois lointain (horloge du producteur déréglée) en est la cause. La réparation (déplacer
ces lignes) est une intervention **manuelle et relue**, pas un `DELETE` : l'événement reste
valable, seule sa partition change.

**Intégrité (chaîne de hachage).** `python3 scripts/verify_chain.py [topic]` recalcule la chaîne de
chaque partition Kafka et signale le premier offset rompu (code de sortie 1 si une rupture existe).
À lancer après toute intervention manuelle sur la base, et périodiquement. Elle prouve la
**cohérence** (une ligne modifiée ou retirée au milieu est vue), pas la **complétude** (des lignes
retirées à la toute fin ne le sont pas) : voir ADR-015.

**Mots de passe.** Ils sont posés à la **première** création du volume. Changer `.env`
ensuite ne change pas les rôles : `docker compose exec qm-audit psql -U qm_admin -d qm_audit -c
"ALTER ROLE audit_writer PASSWORD '…'"`, puis redémarrer le service concerné.

**Tests d'acceptation** (pile de dev lancée ; les lignes de test restent, la table est
append-only) : `scripts/crash_test.sh`, `scripts/test_privileges.sh`, `scripts/audit_e2e.sh`,
`scripts/test_migrations.sh` (base jetable, n'altère pas la pile).

## 6. Kafka (lot 01)

| Quoi | Comment |
|---|---|
| Créer / aligner les topics | automatique à chaque `up` (service `topics-init`) ; à la main : `docker compose run --rm topics-init` |
| Voir ce que fait `topics.sh` | il affiche `created`, `altered`, `REFUSED`, et `topics: changed=N` (0 = rien à faire) |
| Test de fumée | `scripts/smoke.sh` (produit, relit, compare ; vérifie aussi que l'auto-création est coupée) |
| Console AKHQ | `http://127.0.0.1:8181` — depuis Windows : `ssh -L 8181:127.0.0.1:8181 <serveur>` puis navigateur sur `localhost:8181` |
| Décrire les topics | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe` |
| Consommer à la main | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic qm.audit.valuation.v1 --from-beginning --max-messages 5` |

- Kafka n'a **qu'un broker, facteur de réplication 1 : pas de haute disponibilité**
  (ADR-008). Si le volume `kafka-data` est perdu, les messages non encore lus par le
  puits sont perdus ; ce qui est déjà en base d'audit reste.
- Un topic ne peut pas voir son nombre de partitions **réduit**. Si `topics-init`
  affiche `REFUSED`, c'est que le fichier demande moins de partitions que le broker :
  créer un topic `…v<N+1>` (règle du contrat) plutôt que de modifier l'existant.
- Depuis l'hôte, un client Kafka se connecte sur `127.0.0.1:9094` (listener
  `EXTERNAL`) ; depuis un conteneur du réseau `dataplatform`, sur `kafka:9092`.

## 7. Télémétrie et Grafana (lot 03)

Deux chemins parallèles (ADR-004) : les événements d'audit vont dans Kafka, la
télémétrie passe par le **Collector OTel** — l'API lui parle en OTLP sur
`otel-collector:4317` (réseau `dataplatform`) et le Collector la répartit vers
Prometheus (métriques), Loki (logs) et Tempo (traces). Les logs des conteneurs sont lus
directement par le Collector.

| Quoi | Où |
|---|---|
| Grafana | `http://127.0.0.1:3100` (utilisateur `admin`, mot de passe `GRAFANA_ADMIN_PASSWORD` du `.env`) ; depuis Windows : `ssh -L 3100:127.0.0.1:3100 <serveur>` |
| Prometheus / Loki (debug) | `127.0.0.1:9091` / `127.0.0.1:3101` |
| OTLP/HTTP du Collector (tests depuis l'hôte) | `127.0.0.1:4318` |
| Tableaux de bord | dossier *Dashboards* : API, Pricing, Kafka, Sécurité — **définis dans `grafana/dashboards/*.json`** |

**Rien ne se règle à la main dans Grafana.** Les sources de données et les tableaux de
bord sont provisionnés depuis des fichiers et l'interface ne peut pas les écraser. Pour
modifier un tableau de bord : le changer dans l'interface *pour essayer*, puis *Export →
Save to file* et remplacer le JSON dans `grafana/dashboards/`, puis commiter. Un tableau
de bord créé à la main et non rapatrié disparaît au prochain `down -v`.

**Un projet qui rejoint la plateforme** (l'API) : rejoindre le réseau `dataplatform`,
envoyer l'OTLP à `otel-collector:4317`, et mettre dans son compose
`logging: {driver: json-file, options: {labels: com.docker.compose.service}}` pour que ses
logs portent un `service_name`.

**Règle de cardinalité.** Jamais un ticker, un utilisateur, un `request_id` ou une IP
comme étiquette : le Collector les supprime des métriques, et
`scripts/check_labels.sh` le vérifie. Ces valeurs vivent dans les événements d'audit et
les traces.

**Tests** (pile lancée) : `scripts/telemetry_e2e.sh` (OTLP → les trois backends,
suppression des étiquettes interdites, retard du sink qui monte puis retombe),
`scripts/check_labels.sh`, `python3 scripts/check_dashboards.py` (rejoue chaque requête
des tableaux de bord). `scripts/measure_memory.sh` relève la mémoire réelle sous charge.

### Exposer Grafana sur Internet (action manuelle — à faire seulement si tu le veux)

**Rien n'est exposé tant que tu n'as pas fait ces étapes**, et Grafana reste lié à
`127.0.0.1`. Le tunnel Cloudflare existant est géré dans le tableau de bord Cloudflare,
donc c'est à toi de le faire :

1. Cloudflare Zero Trust → *Networks → Tunnels* → ton tunnel → *Public Hostname* → **Add**.
   Nom d'hôte : par exemple `grafana.tramonihadrien.com`. Service : `HTTP` et
   `grafana:3000` **si le conteneur `cloudflared` est sur le réseau `dataplatform`** (il
   faut alors aussi relier Grafana à ce réseau) ; sinon `http://host.docker.internal:3100`.
2. **Avant de sauvegarder l'étape 1** : Zero Trust → *Access → Applications → Add an
   application → Self-hosted*, même nom d'hôte, politique **Allow** limitée à ton adresse
   e-mail. Sans cette application Access, Grafana serait exposé avec pour seule défense
   son mot de passe.
3. Vérifier depuis un navigateur en navigation privée : on doit tomber sur l'écran de
   connexion Cloudflare Access, pas sur Grafana.

Ne jamais publier AKHQ (aucune authentification propre) ; pour le voir, utiliser le
tunnel SSH ci-dessus.

## 8. Qualité des données et alertes (lot 04)

**`data-quality`** est un second groupe de consommateurs (`data-quality`), indépendant du
puits (`audit-sink`) : chacun a ses propres offsets sur les mêmes topics, arrêter l'un ne
touche pas l'autre. Il expose `/metrics` (scruté par Prometheus) : replis par `kind`,
valorisations `clean`/`degraded` (au moins un input non `observed`), statut de chaque input,
et l'instant du dernier événement par source (un producteur muet = cette valeur ne bouge plus).

**Alertes** : `alerts/rules.yml` (7 règles, **chaque seuil justifié en commentaire**),
`alerts/contact-points.yml`, `alerts/policies.yml`, chargées par Grafana au démarrage. Pour
recalibrer un seuil : modifier le fichier, puis `scripts/up.sh` (dev) ou `scripts/deploy.sh`
(prod) — jamais dans l'interface.

**Canal de notification — pas encore choisi.** Les alertes passent en *firing* dans Grafana
(*Alerting → Alert rules*) mais n'avertissent personne tant que `ALERT_WEBHOOK_URL` est vide.
Quand tu auras choisi (ntfy auto-hébergé recommandé : notification native sur Windows et
téléphone) : ajouter le service au compose, mettre l'URL dans `.env`, `scripts/deploy.sh`.
Le point de contact est un webhook générique : ntfy, un pont Pushover ou un webhook de chat
fonctionnent sans changer le code.

**Changer une configuration** (`prometheus/`, `loki/`, `tempo/`, `otel/`, `grafana/`,
`alerts/`, `akhq/`) : `deploy.sh` (ou `scripts/up.sh` en dev) hache ces fichiers et recrée
**uniquement** les services dont la configuration a changé. Un simple `docker compose up -d`
ne le ferait pas : Docker ne voit pas qu'un fichier monté a changé.

**Tests** (pile lancée) : `scripts/dq_crash_test.sh`, `scripts/dq_independence_test.sh`,
`scripts/alert_e2e.sh` (≈ 8 min : provoque un repli, attend *firing* puis *normal*, et vérifie
que Grafana poste les notifications à un récepteur local jetable).

**Mesure de fin de chantier** (avec `quant-modeling`) : `audit.v_fallbacks_daily` doit être
**vide** sur une semaine de trafic normal, une fois les replis retirés des anciens endpoints
(`vol_surface.py`, `local_vol_pricing.py`, `simulation.py`). Tant qu'elle ne l'est pas, le
tableau de bord *Pricing* montre l'écart restant.

## 9. Registre de schémas (lot 05)

Apicurio Registry (`schema-registry`), console et API sur `http://127.0.0.1:8082` (depuis
Windows : tunnel SSH `-L 8082:127.0.0.1:8082`). Compatibilité globale **`BACKWARD`**, posée à
chaque `up` par `registry-init`. Format des messages et règles d'évolution : `docs/contract.md` §5.

- **État** : dans le topic Kafka `kafkasql-journal` (rétention illimitée). **Ne jamais le
  supprimer ni réduire sa rétention** : chaque message Avro contient l'identifiant d'un schéma
  que seul ce journal connaît. `docker compose down` (sans `-v`) et les redémarrages le
  conservent ; `down -v` le détruit — les schémas se ré-enregistrent depuis les producteurs,
  mais avec de **nouveaux identifiants**, donc les anciens messages Avro encore dans Kafka
  deviennent illisibles (le sink les envoie en DLQ). La base d'audit, elle, n'est pas touchée.
- **Sauvegarde** : `scripts/backup_registry.sh` (export zip de tous les schémas), lancé avec
  la sauvegarde d'audit. Restauration : console → *Import* du zip, ou
  `POST /apis/registry/v3/admin/import`.
- **Si le registre est en panne** : les messages JSON continuent de passer ; les messages Avro
  dont le schéma n'est pas déjà en cache attendent (le sink réessaie, le retard augmente, rien
  n'est perdu ni mis en DLQ). Le relancer suffit.
- **Un producteur reçoit un 409** en enregistrant son schéma : le message nomme le champ fautif.
  Ajouter le champ avec une valeur par défaut, ou créer un topic `…v2`.

**Test** (pile lancée) : `.venv/bin/python scripts/registry_test.py` (enregistrement, évolution,
schéma incompatible refusé, taille JSON/Avro, survie au redémarrage, panne du registre).

## 10. Laboratoire d'exercices (lot 06)

`docker-compose.lab.yml` est un **projet Compose distinct** (`quant-platform-lab`) : ses propres
volumes, son propre réseau, aucun port publié, **jamais** relié à `dataplatform`. Les exercices y
tuent des brokers et réécrivent des offsets ; ils ne touchent pas la plateforme.

```bash
scripts/lab_up.sh              # grappe de 3 brokers (vérifie d'abord l'isolation, refuse sinon)
scripts/lab_up.sh replay       # + sink et Postgres jetables (rejeu à grande échelle)
scripts/lab_down.sh            # détruit conteneurs, volumes et réseau, et VÉRIFIE qu'il ne reste rien
```

Les exercices et leurs **observations réelles** sont dans [`exercices.md`](exercices.md) (partie B).
Le lab consomme ≈ 2 Go de RAM quand il tourne avec la grappe ; le détruire dès qu'on a fini.

## 11. Vérifier la santé

```bash
./scripts/make.sh                        # (dev) lint + validation + tests
docker compose ps                        # état des services
docker stats --no-stream                 # mémoire réelle vs plafonds
```
