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

## 5. Sauvegarde et restauration

*Point d'attache — livré au lot 02 :* un timer systemd (`pg_dump` de la base
d'audit) sur le modèle de `quant-modeling-backup.timer`. Cette section sera
complétée avec la procédure de restauration testée.

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

## 7. Vérifier la santé

```bash
./scripts/make.sh                        # (dev) lint + validation + tests
docker compose ps                        # état des services
docker stats --no-stream                 # mémoire réelle vs plafonds
```
