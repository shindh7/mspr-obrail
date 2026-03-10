# Documentation API REST - ObRail Europe

## Base URL
Par defaut: `http://127.0.0.1:8000`

## Endpoints principaux

### 1) Trips
`GET /trips`

Parametres:
- `country` (code pays)
- `operator` (nom ou code)
- `type_transport` (train|avion)
- `train_kind` (tgv|rer|intercite)
- `departure` / `arrival`
- `service_date` (YYYY-MM-DD) pour conversion en CET/CEST
- `limit` / `offset`

Exemples:
```
GET /trips?type_transport=train&country=FR&limit=50
GET /trips?type_transport=train&service_date=2026-03-01
```

### 2) Stations
`GET /stations`

Parametres:
- `country`
- `search`
- `limit` / `offset`

### 3) Vehicules
`GET /vehicules`

Parametres:
- `type_transport`
- `specificite`
- `train_type`

### 4) Coverage (stats)
`GET /coverage`
`GET /stats/coverage`

Parametres:
- `type_transport`

### 5) Qualite
`GET /stats/quality`

Parametres:
- `type_transport`

## Reponse JSON
Toutes les reponses sont JSON. En cas d erreur, l API renvoie un HTTP 4xx/5xx avec un champ `detail`.

Si `service_date` est fourni:
- `departure_time_cet`, `arrival_time_cet`
- `departure_date_cet`, `arrival_date_cet`

## Tests
- Postman (collections a produire si besoin)
- curl (exemples ci-dessus)
