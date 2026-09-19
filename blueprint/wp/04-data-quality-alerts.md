# WP 04 — Qualité des données et alertes

| | |
|---|---|
| **Dépend de** | [02](02-audit-sink.md), [03](03-telemetry.md) |
| **Bloque** | la mesure de fin du chantier « plus de repli en direct » de `quant-modeling` |
| **Branche** | `wp/04-data-quality-alerts` |
| **Référence** | *Kafka: The Definitive Guide* ch. 4 (groupes multiples) ; documentation Grafana Alerting ; SR 11-7 (contrôle de la qualité des inputs de modèle) |

## Objectif

Deux choses : (1) un **second consommateur indépendant** du même flux qui agrège les
replis et publie des métriques ; (2) des **alertes en fichiers**, avec un canal de
notification qui joint réellement le mainteneur.

## Ce qu'on apprend

| Notion | Où |
|---|---|
| **Second groupe de consommateurs** sur le même topic, sans coordination avec le premier | `data_quality/` (groupe `data-quality`) |
| Agrégation **par fenêtre** sur un flux | comptage des replis par fenêtre glissante |
| Le **vocabulaire desk** des inputs : `observed`, `stale`, `proxied`, `default` | tableau de bord de qualité |
| **Alertes en code** : règle, seuil, fenêtre, sévérité, canal | `alerts/` |

## Tâches

1. **`data_quality/`** : consommateur Python (groupe **`data-quality`**, distinct de
   `audit-sink`) sur `qm.dataquality.fallback.v1` et `qm.audit.valuation.v1`. Il
   maintient et expose (`/metrics`) : replis par `kind`, part des valorisations
   dont au moins un input n'est pas `observed`, **âge** du dernier événement par
   source (détecter un producteur muet). Même discipline que le sink : commit
   manuel, arrêt propre, test de plantage.
2. **Règles d'alerte** (fichiers dans `alerts/`, chargées par le provisioning
   Grafana) :

   | Alerte | Condition (à calibrer, à documenter) |
   |---|---|
   | Repli en direct | tout `data.fallback` ≠ 0 sur la fenêtre |
   | Rafale d'échecs de connexion | N échecs depuis un même hachage d'IP sur T minutes |
   | Erreurs serveur | taux de 5xx au-dessus d'un seuil |
   | Retard du sink | *lag* du groupe `audit-sink` au-delà d'un seuil |
   | Événements perdus | `qm_audit_dropped_total` > 0 |
   | Producteur muet | aucun événement depuis T alors que du trafic HTTP existe |

   **Chaque seuil est justifié dans le fichier** (commentaire : pourquoi cette
   valeur), pas posé au jugé.
3. **Canal de notification.** Le mainteneur travaille depuis un client Windows en
   Remote-SSH vers un serveur sans écran et veut une **notification native**.
   **Recommandation** : un serveur **`ntfy`** auto-hébergé (conteneur, gratuit)
   comme *contact point* Grafana par webhook, avec l'application ou le navigateur
   ntfy côté Windows. **À confirmer avec le mainteneur avant de construire** ; il
   pourra préférer l'e-mail. Ne rien choisir sans son accord.
4. **Test de bout en bout** : un script provoque un repli (côté producteur ou par un
   événement injecté), vérifie que l'alerte passe à l'état *firing* puis revient à
   *normal*.
5. **Élargissement (hors périmètre, à noter)** : `data-ingest` pourrait publier des
   événements de fraîcheur d'ingestion (`ingest.run.completed`) ; le contrôle qualité
   saurait alors quand une table de marché est réellement périmée, au lieu de le
   déduire. À ouvrir en issue dans `data-ingest`, pas ici.

## Critères d'acceptation

- **Test de plantage** du consommateur `data-quality`, comme celui du sink.
- **Indépendance des groupes** : arrêter `data-quality` n'affecte ni le sink ni son
  retard ; arrêter le sink n'affecte pas `data-quality`. Vérifié par script.
- Provoquer un repli déclenche l'alerte et la **notification arrive
  réellement** sur le canal choisi ; l'arrêter l'éteint.
- Les alertes sont **rechargées depuis les fichiers** après `down -v && up -d`.
- **Mesure de fin de chantier** : `v_fallbacks_daily` est **vide** sur une semaine de
  trafic normal, *une fois les replis retirés des anciens endpoints de*
  `quant-modeling` (`vol_surface.py`, `local_vol_pricing.py`, `simulation.py`).
  Tant qu'il ne l'est pas, le tableau de bord montre l'écart restant.

## Exercices

1. Lancer trois instances de `data-quality` dans le même groupe, en tuer une et
   observer le rééquilibrage dans AKHQ.
2. Calculer le taux de repli sur une fenêtre **par requête SQL** dans `qm-audit`,
   puis par la métrique, et vérifier qu'ils concordent ; comprendre la différence
   entre les deux chemins (ADR-004).

## Fichiers créés

`data_quality/` (code, `requirements.in`/`.txt`, `Dockerfile`, tests), `alerts/*.yml`,
`docker-compose.yml` (services `data-quality`, `ntfy` selon décision),
`scripts/alert_e2e.sh`.
