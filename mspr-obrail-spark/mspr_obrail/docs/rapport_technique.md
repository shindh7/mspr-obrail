# Rapport technique - ObRail Europe (MSPR ETL)

## 1. Contexte et objectifs
Le projet vise a construire un entrepot de donnees unifie pour analyser les dessertes ferroviaires
europeennes et comparer l impact des trains de jour et de nuit. Les objectifs principaux sont:
- collecter des donnees heterogenes (GTFS, CSV, XLSX, parquet),
- fiabiliser et normaliser les donnees,
- stocker dans une base relationnelle exploitable,
- exposer via une API REST,
- fournir un tableau de bord de controle de qualite.

## 2. Perimetre du lot MSPR
Inclus:
- extraction multi-sources et automatisation ETL,
- modelisation de donnees (MCD/MPD),
- base PostgreSQL alimentee,
- API REST documentee,
- tableau de bord de controle (web).

Hors perimetre:
- entrainement de modeles IA,
- mise en production cloud,
- interfaçage avec SI client.

## 3. Sources de donnees et justification
| Source | Format | Usage | Justification |
| --- | --- | --- | --- |
| Catalogue GTFS (open data) | CSV + ZIP | reseaux ferroviaires | standard international, large couverture |
| GTFS statiques (selection) | ZIP | reseaux critiques | fiabilite sur certains pays |
| Trains de nuit (CSV/XLSX) | CSV/XLSX | service nuit | enrichit l analyse specifique nuit |
| Vols commerciaux (parquet) | Parquet | benchmark avion | comparaison CO2 vs rail |

## 4. Architecture ETL
### 4.1 Extraction
- telechargement et validation des sources,
- gestion d ignorelist,
- decompression GTFS,
- options de chargement (catalog-only, snapshot-only).

### 4.2 Transformation
- normalisation des colonnes et formats,
- nettoyage coordonnees et pays,
- deduplication A->B / B->A,
- enrichissement: distance, CO2, type transport,
- regles metier (train nuit, type train).

### 4.3 Chargement
- ecriture PostgreSQL (JDBC),
- snapshots pour rejouer et fusionner les runs.

## 5. Modele de donnees
Le schema est detaille dans `docs/mcd_mpd.md` et `data/scripts/mart/schema_transport.sql`.
Tables principales:
- `vehicule` (dimension),
- `station` (dimension),
- `trajet` (faits).

## 6. Qualite des donnees et tableau de bord
Tableau de bord controle:
- completness des trajets (pays, coordonnees, distance, CO2),
- stations sans pays ou coordonnees,
- vehicules sans specificite.
Voir `docs/tableau_de_bord.md`.

## 7. API REST (exposition des donnees)
Endpoints principaux:
- `/trips`, `/stations`, `/vehicules`, `/operators`,
- `/coverage`, `/stats/coverage`, `/stats/quality`.
Filtrage par pays, transport, type de train, etc.
Voir `docs/api.md`.

## 8. Automatisation et reproductibilite
Parametrage par variables d environnement:
- selection des pays, max GTFS par pays,
- activation/desactivation des sources,
- snapshots (load/merge/write),
- options CO2 et qualite.
Voir `docs/etl.md`.

## 9. Conformite RGPD
Les donnees traitees sont publiques et ne contiennent pas de donnees personnelles.
Mesures appliquees:
- tracabilite des sources,
- journaux d execution,
- documentation des transformations.

## 10. Tests et validation
- verifications SQL (volumes, doublons, valeurs manquantes),
- tests manuels API via Postman,
- controles qualite via dashboard.

## 11. Limites et pistes d amelioration
- couverture inegale selon pays,
- horaires optionnels (stop_times),
- ajout de tests automatises ETL/API,
- enrichissement des sources et metadonnees.
