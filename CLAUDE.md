# quant-platform — notes pour Claude Code

Plateforme **auto-hébergée** d'événements, de piste d'audit et d'observabilité
pour les projets de `~/` — d'abord `quant-modeling` (pricing de dérivés), puis
`data-ingest` et les autres. Elle apporte : un bus **Kafka**, une **base
d'audit append-only**, la **télémétrie** (OpenTelemetry, Prometheus, Loki,
Tempo, Grafana) et les **alertes**.

C'est aussi un **chantier d'apprentissage** déclaré : le mainteneur veut monter
ce que montent les desks quants (bus d'événements, piste d'audit, gouvernance
des valorisations). Quand le choix simple et le choix « comme sur un desk »
divergent, on prend le second **s'il est expliqué** — et on dit honnêtement
quand un composant est surdimensionné pour le volume (Kafka l'est).

**Ce dépôt ne contient aucun code de pricing.** Il ne contient pas non plus
l'instrumentation de l'API : celle-ci vit dans `~/quant-modeling`
(`api/app/audit/`). Voir « Relation avec les autres dépôts ».

## État du dépôt

Le dépôt est né d'un document de conception écrit côté `quant-modeling`
(`~/quant-modeling/blueprint/wp/18-observability.md`, qui reste la vue
d'ensemble des deux dépôts). **Au moment de la création, il ne contient que de
la documentation** : rien n'est encore construit. Le travail est découpé en lots
dans [`blueprint/wp/`](blueprint/wp/) ; le graphe de dépendances est dans
[`blueprint/README.md`](blueprint/README.md) — le consulter avant de démarrer un
lot pour vérifier que ses prérequis sont faits.

## Relation avec les autres dépôts

| Dépôt | Rôle vis-à-vis d'ici |
|---|---|
| `~/quant-modeling` | **Producteur** d'événements (API FastAPI). Possède les **schémas de payload** et l'endpoint de *replay*. Sa session Claude est séparée de celle-ci |
| `~/data-ingest` | Postgres de **données de marché** (`data-ingest-postgres`). Reste distinct de la base d'audit. Futur producteur (événements de fraîcheur d'ingestion) |
| `~/AgenticEnv` | Autre projet du serveur ; pourrait à terme produire des événements |
| réseau Docker `dataplatform` | **Point de jonction** : réseau externe partagé. Tout ce qui doit se parler s'y raccorde par nom de conteneur |

**Règles de frontière, à ne pas contourner :**

- **Ne jamais éditer `~/quant-modeling` depuis une session ouverte ici** (ni
  l'inverse). Un changement qui touche les deux côtés = une issue dans chaque
  dépôt, référencées l'une par l'autre, et deux PR coordonnées.
- **Le contrat** entre les deux (noms de topics, enveloppe d'événement, rôles) est
  dans [`docs/contract.md`](docs/contract.md). Il est **canonique ici** ; toute
  modification est un ADR dans [`blueprint/decisions.md`](blueprint/decisions.md).
- Les **payloads** appartiennent au producteur. Ce dépôt ne modifie pas un schéma
  de payload ; il en est consommateur.

## Principes d'architecture non négociables

1. **Deux chemins parallèles.** Les **événements métier dont on doit répondre**
   (valorisations, authentification, replis, assistant) vont dans **Kafka**. La
   **télémétrie** (métriques, logs, traces) passe par **OpenTelemetry**, jamais
   par Kafka. La télémétrie tolère de perdre un échantillon, la piste d'audit non.
2. **L'API n'a aucun identifiant de la base d'audit.** Elle produit dans Kafka,
   point. Seul `audit-sink` écrit en base. Une API compromise ne peut qu'ajouter
   des événements, pas réécrire l'historique.
3. **La base d'audit est append-only.** Personne n'a `UPDATE` ni `DELETE`. La
   rétention se fait en détachant puis supprimant une **partition mensuelle
   entière**, jamais ligne à ligne.
4. **Au moins une fois + puits idempotent** (clé `event_id`), pas de transactions
   Kafka de bout en bout.
5. **Kafka n'est pas l'archive** : la source de vérité durable est Postgres.
6. **L'absence de la plateforme ne casse jamais le site.** L'API se replie sur un
   spool local. Rien ici ne doit devenir un point de défaillance du pricing.
7. **Tout est du code.** Topics, tableaux de bord, alertes, schémas SQL, rôles :
   des fichiers versionnés, pas de la configuration faite à la main dans une
   interface. `docker compose down -v && up -d` doit tout reconstruire.
8. **Cardinalité** : jamais un ticker, un utilisateur, un `request_id` ou une IP
   comme **étiquette** Prometheus ou Loki. Ces valeurs vivent dans les événements
   et les traces.
9. **Aucune donnée personnelle en clair dans l'audit** : l'IP est hachée
   (`HMAC-SHA256`) à l'émission par le producteur.

## Arborescence cible

```
docker-compose.yml        # la plateforme (réseau externe `dataplatform`)
.env.placeholder          # copié en .env, jamais commité
topics/topics.yml         # topics en code + script idempotent de création
migrations/               # SQL versionné du schéma `audit`, des rôles, des vues
sink/                     # audit-sink (Python, confluent-kafka + psycopg)
data_quality/             # consommateur de contrôle qualité (lot 04)
otel/  prometheus/  loki/  tempo/
grafana/                  # sources de données + tableaux de bord provisionnés
alerts/                   # règles d'alerte en fichiers
scripts/                  # deploy.sh, smoke.sh, make.sh…
docs/                     # contract.md, exercices.md, RUNBOOK.md
blueprint/                # lots de travail, décisions
```

Ne créer un dossier que quand un lot le remplit.

## Commandes

Ces commandes sont la **cible** ; chacune n'existe qu'une fois son lot livré
(le lot est indiqué). Ne pas les présenter comme disponibles avant.

| | Lot |
|---|---|
| `docker compose up -d` — la plateforme | 00, 01 |
| `scripts/smoke.sh` — produit un événement, le relit ; puis producteur → Kafka → sink → Postgres | 01, 02 |
| `scripts/make.sh` — lint + validation compose + tests | 00 |
| `pytest` — tests du sink et du contrôle qualité | 02 |
| `scripts/deploy.sh` — depuis le dossier de prod uniquement | 00 |
| `pip-compile` — régénère les `requirements*.txt` figés depuis les `.in` | 02 |

## Suivi du travail — GitHub Issues, pas de markdown de handoff

Le « JIRA » du projet, ce sont les **GitHub Issues du dépôt** (compte
`HadrienT`, `gh` est authentifié) — **dès que le dépôt distant existe** ; tant
qu'il n'existe pas, le dire au mainteneur plutôt que d'improviser.

- Au démarrage d'une session : `gh issue list --state open`.
- Issue traitée → `gh issue close <n> --comment "fait dans <sha>"`.
- **Jamais** de fichier markdown de passation entre sessions : une tâche qui
  survit à la session est une issue.
- Les gros morceaux de conception vivent dans `blueprint/wp/*.md` ; l'issue y
  renvoie, elle ne les remplace pas.

## Documents de référence

| Fichier | Rôle |
|---|---|
| [`blueprint/README.md`](blueprint/README.md) | Index des lots, graphe de dépendances, correspondance avec les lots 18a–18h de `quant-modeling` |
| [`blueprint/wp/`](blueprint/wp/) | Un lot par fichier : périmètre, tâches, **critères d'acceptation vérifiables par une commande** |
| [`blueprint/decisions.md`](blueprint/decisions.md) | ADR : chaque choix avec ses alternatives écartées |
| [`docs/contract.md`](docs/contract.md) | Le contrat producteur ↔ plateforme (topics, enveloppe, rôles SQL) |
| `~/quant-modeling/blueprint/wp/18-observability.md` | Vue d'ensemble des deux dépôts (lecture seule depuis ici) |

## Conventions

- **Conversation avec le mainteneur : en français.** Code, identifiants,
  commentaires, messages de commit : en anglais. `blueprint/` et `docs/` sont
  en français.
- **Commits : passer par une branche, jamais directement sur `main`** (le tout
  premier commit du dépôt, qui pose la documentation, est la seule exception).
  Terminer chaque message de commit par la ligne `Co-Authored-By` fournie par la
  session Claude Code en cours.
- **Un lot n'est pas terminé tant que ses critères d'acceptation ne sont pas
  vérifiés par la commande qui les décrit.** « Ça a l'air de marcher » n'est pas
  un critère. Si un test échoue ou une étape est sautée, le dire tel quel.
- **Python** : `black` ; dépendances déclarées dans des `requirements*.in`,
  figées par `pip-compile` (comme dans `quant-modeling`), pas installées à la
  main une à une. **SQL** : migrations numérotées, rejouables, jamais modifiées
  après application (on ajoute une migration). **Shell** : `set -euo pipefail`,
  `shellcheck` propre. **YAML** : `yamllint`.
- **Docker** : versions d'images **épinglées** (jamais `latest`) ; **limites de
  mémoire et de CPU explicites** sur chaque service ; **aucun port publié hors
  `127.0.0.1`** ; Kafka et la base d'audit ne sont joignables que par le réseau
  `dataplatform`.
- **Secrets** : jamais dans git. `.env` est ignoré, `.env.placeholder` documente
  les variables.
- Tout composant qui traite des événements arrive avec son **test de plantage**
  (on tue le processus au milieu d'un flux, on relance, on compte : ni perte ni
  doublon) — c'est l'équivalent ici du test de propriété du cœur C++.

## Le mainteneur — ce qu'il faut savoir avant d'agir

- **Il ne maîtrise pas git en profondeur ni le déploiement.** Avant toute
  manipulation de branches, de worktrees, de merges, ou tout déploiement,
  **expliquer** ce que fait chaque commande et ce qui est réversible, en termes
  simples ; préférer le chemin le plus simple (fast-forward, une commande claire
  à la fois). Une fois le plan expliqué, il attend que le travail soit mené
  jusqu'au bout.
- **Il veut apprendre.** Expliquer le *pourquoi* de chaque notion Kafka, SQL ou
  observabilité au moment où on l'utilise ; proposer les exercices prévus dans
  les lots.
- **Dépendances : déclaratives et clés en main**, pas d'installations manuelles
  une par une (`apt-get install` isolé toléré comme déblocage, signalé comme tel).
- **Matériel partagé.** Le serveur a ~56 cœurs et ~47 Go de RAM, mais cette RAM
  est partagée avec d'autres projets et avec les calculs lourds de
  `quant-modeling` (~20 Go pour un calcul AAD). Tout dimensionnement se **calcule
  et se mesure** (`docker stats`), il ne se devine pas ; d'où les plafonds
  explicites.
- **Auto-hébergé, pas de cloud** : pas de service managé, pas de GCP.
- **Notifications** : le mainteneur travaille depuis un client Windows en
  Remote-SSH vers ce serveur sans écran ; il souhaite être alerté (blocage,
  fin de tâche) par une notification native, ce qui passe par un pont hors de la
  machine (ntfy, Pushover…) — à choisir, voir lot 04.

## Déploiement

Même modèle que `quant-modeling` : deux dossiers, **un seul dépôt git**
(`git worktree`).

- `~/quant-platform` : **développement** (branches, commits, PR). C'est ici que
  travaillent les sessions Claude.
- `~/quant-platform-prod` : **la plateforme en ligne, toujours sur `main`**. Ne
  sert qu'à `./scripts/deploy.sh` (`git pull --ff-only`, puis
  `docker compose up -d`). **Ne jamais éditer ni committer depuis ce dossier.**
  Il n'existe pas encore : sa création est dans le [lot 00](blueprint/wp/00-foundations.md).

**Redéployer l'API ne redémarre pas la plateforme, et inversement.** Au boot, la
plateforme démarre d'abord ; l'API démarre sans elle (spool). Un merge dans
`main` ne change rien en ligne tant que `deploy.sh` n'est pas lancé.

## Ce qu'il ne faut pas faire

- Ajouter du code de pricing, ou lire la base de marché de `data-ingest`.
- Router de la télémétrie par Kafka, ou donner à l'API un accès à la base d'audit.
- Un `UPDATE` ou un `DELETE` dans le schéma `audit`, même « juste une fois ».
- Un tableau de bord ou une alerte créé à la main dans l'interface Grafana sans
  le rapatrier en fichier.
- Publier un port sur `0.0.0.0`, ou exposer Grafana / AKHQ publiquement sans
  Cloudflare Access.
- Une étiquette de métrique à forte cardinalité.
