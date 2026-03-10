# Support de soutenance - trame

## Slide 1 - Contexte et objectifs
- ObRail Europe: observatoire ferroviaire europeen.
- Besoin: entrepot unifie et analyse jour/nuit.
- Objectif: ETL + API + dashboard.

## Slide 2 - Sources et contraintes
- Sources heterogenes (GTFS, CSV, XLSX, parquet).
- Qualite variable, harmonisation necessaire.
- Contraintes RGPD et delais.

## Slide 3 - Architecture ETL
- Extraction: catalog GTFS, sources statiques, fichiers.
- Transformation: nettoyage, filtrage, dedup.
- Chargement: PostgreSQL + snapshots.

## Slide 4 - Modele de donnees
- MCD/MPD (3 tables principales).
- Raison du schema en etoile.

## Slide 5 - Qualite et dashboard
- Controles: pays manquants, coords, CO2.
- Tableau de bord Quality.

## Slide 6 - API REST
- Endpoints clefs: /trips, /coverage, /stats.
- Filtrage par transport/type/pays.
- Exemples rapides.

## Slide 7 - Resultats
- Volumes (trajets, pays).
- Exemples de comparatif CO2.

## Slide 8 - Limites et perspectives
- Enrichissement sources.
- Tests automatises.
- Evolutions IA.

## Annexes (backup)
- Schema SQL.
- Variables d environnement.
