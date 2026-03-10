# Base de donnees - ObRail Europe

## SGBD
PostgreSQL (schema par defaut: `obrail_transport`)

## Creation du schema
Script: `data/scripts/mart/schema_transport.sql`

## Chargement
L ETL alimente les tables suivantes:
- `station`
- `vehicule`
- `trajet`

Colonnes optionnelles:
- `trajet.departure_time`, `trajet.arrival_time`
- `trajet.agency_timezone`

## Verification rapide
```sql
SELECT COUNT(*) FROM obrail_transport.station;
SELECT COUNT(*) FROM obrail_transport.vehicule;
SELECT COUNT(*) FROM obrail_transport.trajet;
```

## Nettoyage
```sql
TRUNCATE obrail_transport.trajet, obrail_transport.vehicule, obrail_transport.station CASCADE;
```
