# WP 00 — Fondations

| | |
|---|---|
| **Dépend de** | rien |
| **Bloque** | tous les autres lots |
| **Branche** | `wp/00-foundations` |
| **Référence** | `~/quant-modeling/deploy/RUNBOOK.md`, `~/quant-modeling/scripts/deploy.sh`, `~/data-ingest/docker-compose.yml` (les deux motifs à reproduire) |

## Objectif

Poser ce qui rend les lots suivants **vérifiables et déployables** avant d'y
mettre le moindre service : un contrôle qualité qui casse quand il faut, une CI, et
le circuit branche → PR → `main` → `deploy.sh`, identique à celui de
`quant-modeling`.

## Ce qu'on apprend

- Le motif **deux dossiers, un dépôt** (`git worktree`) pour séparer développement
  et production.
- Valider une infra **avant** de la lancer : `docker compose config`, `yamllint`,
  `shellcheck`.
- Un réseau Docker **externe** comme couture entre projets.

## Tâches

1. **`docker-compose.yml` minimal**, `name: quant-platform`, avec le réseau
   externe :

   ```yaml
   networks:
     dataplatform:
       external: true
   ```

   Pas encore de service (les lots suivants les ajoutent). Si Compose refuse un
   fichier sans service, ajouter un service de test temporaire retiré au lot 01.
2. **`scripts/make.sh`** : `set -euo pipefail` ; `docker compose config -q`,
   `yamllint`, `shellcheck scripts/*.sh`, `black --check` (dès qu'il y a du
   Python). Un seul point d'entrée, utilisé à l'identique par la CI.
3. **CI GitHub Actions** (`.github/workflows/ci.yml`) : lance `scripts/make.sh` sur
   chaque PR. Versions d'outils épinglées.
4. **`scripts/deploy.sh`**, sur le modèle de celui de `quant-modeling` :
   idempotent, sûr au boot, `git pull --ff-only` non fatal hors ligne, crée le
   réseau `dataplatform` s'il manque, `docker compose up -d`, attend que les
   services soient `healthy`.
5. **Dossier de prod** : `git worktree add ~/quant-platform-prod main`. Une même
   branche ne peut être extraite que dans un seul dossier à la fois ; c'est
   pourquoi le dossier de développement **ne reste pas** sur `main` (voir la
   note ci-dessous).
6. **Unité systemd** de démarrage au boot, sur le modèle de
   `~/quant-modeling/deploy/quant-modeling.service`, avec l'**ordre voulu** : la
   plateforme démarre **avant** l'API.
7. **`docs/RUNBOOK.md`** : mise en route à froid, déploiement, restauration.
8. **Sauvegarde** de la base d'audit : *reportée au lot 02*, qui crée la base ;
   noter ici le point d'attache (timer systemd).

> **Note pour la session qui exécute ce lot — explique avant d'agir.** Le
> mainteneur n'est pas à l'aise avec `git worktree`. Avant l'étape 5, expliquer :
> « un worktree est un second dossier de travail attaché au même dépôt, chacun sur
> une branche différente ; le dossier de prod restera sur `main` et ne sert qu'à
> déployer ». Le dossier de développement est **déjà** sur une branche de lot
> (pas sur `main`) précisément pour que ce worktree puisse exister.

## Critères d'acceptation

- `scripts/make.sh` sort en 0 sur un arbre propre, et **en non-zéro** si on casse
  volontairement un YAML ou un script shell (à tester, pas à supposer).
- La CI est verte sur une PR de ce lot et rouge sur une PR qui casse le YAML.
- `git worktree list` montre `~/quant-platform` sur une branche et
  `~/quant-platform-prod` sur `main`.
- `~/quant-platform-prod/scripts/deploy.sh` lancé deux fois de suite : la seconde
  ne change rien (idempotence).
- Le RUNBOOK permet à quelqu'un qui n'a jamais vu le dépôt de démarrer la
  plateforme à froid.

## Fichiers créés

`docker-compose.yml`, `scripts/make.sh`, `scripts/deploy.sh`, `.github/workflows/ci.yml`,
`deploy/quant-platform.service`, `docs/RUNBOOK.md`, `.yamllint`.
