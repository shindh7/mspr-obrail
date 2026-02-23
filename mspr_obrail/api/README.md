# ObRail Europe API (FastAPI)

## Démarrage
Variables d'environnement PostgreSQL :
- PGHOST
- PGPORT
- PGDATABASE
- PGUSER
- PGPASSWORD

Exemple :
```
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=obrail_europe
export PGUSER=shindh
export PGPASSWORD=*****
```

Installation :
```
pip install -r api/requirements.txt
```

Lancement :
```
uvicorn api.main:app --reload
```

## Endpoints
- GET `/health`
- GET `/trips?is_night=true&country_code=FR&operator_id=sncf_voyageurs&departure_station=Paris&arrival_station=Lyon&limit=100&offset=0`
- GET `/countries`
- GET `/operators`
- GET `/stats/coverage`

## Exemples de requêtes
- Trains de nuit en France (limités à 50)
	- `/trips?is_night=true&country_code=FR&limit=50`
- Trajets Paris -> Lyon (tous opérateurs)
	- `/trips?departure_station=Paris&arrival_station=Lyon`
- Trajets d’un opérateur spécifique
	- `/trips?operator_id=sncf_voyageurs`

## OpenAPI / Swagger
- http://localhost:8000/docs
- http://localhost:8000/redoc

## Interface web
- http://localhost:8000/

## Notes
Requêtes SQL paramétrées, indexées via le schéma du data mart.
