# Guide ETL - ObRail Europe

## Objectif
Automatiser la collecte, transformation et chargement des donnees ferroviaires
et aeriennes dans PostgreSQL.

## Script principal
`data/scripts/stream/stream_etl_spark_transport.py`

## Modes d execution
- Catalogue uniquement
- Snapshot uniquement
- Sources statiques uniquement
- Mix (catalogue + statics + fichiers)

## Variables d environnement cles
- `GTFS_COUNTRY_CODES` : liste des pays
- `GTFS_MAX_SOURCES_PER_COUNTRY` : nb max de GTFS par pays
- `GTFS_DISABLE_CATALOG` : desactiver le catalogue
- `GTFS_INCLUDE_STATIC_SOURCES` : activer les GTFS statiques
- `GTFS_XLSX_URL` : source XLSX
- `GTFS_STOP_TIMES_MODE` : `off` (defaut) ou `ends` (premier/dernier stop + heures)
- `FLIGHTS_ENABLE` : inclure les vols
- `ETL_TRUNCATE` : vider les tables avant chargement
- `ETL_SNAPSHOT_LOAD` / `ETL_SNAPSHOT_MERGE` / `ETL_SNAPSHOT_WRITE`
- `SPARK_MASTER`, `SPARK_SHUFFLE_PARTITIONS`
- `JDBC_WRITE_PARTITIONS`, `JDBC_BATCH_SIZE`

## Exemples

### Catalogue uniquement
```
GTFS_DISABLE_CATALOG=0
GTFS_INCLUDE_STATIC_SOURCES=0
GTFS_XLSX_URL=""
INPUT_FILES=""
FLIGHTS_ENABLE=0
```

### Snapshot uniquement
```
ETL_SNAPSHOT_LOAD=run_full_15629
ETL_SNAPSHOT_MERGE=0
```

## Verification
Voir `docs/base_de_donnees.md`.
