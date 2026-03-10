# Livrables MSPR - ObRail Europe

## Liste des livrables (selon le sujet)
1. Scripts ETL operationnels
2. MCD + MPD (modele conceptuel et physique)
3. Base de donnees relationnelle alimentee
4. API REST fonctionnelle + documentation
5. Documentation technique complete (rapport)
6. Tableau de bord de controle
7. Support de soutenance

## Correspondance avec le projet
- Scripts ETL: `data/scripts/stream/stream_etl_spark_transport.py`
- MCD/MPD: `docs/mcd_mpd.md` + `data/scripts/mart/schema_transport.sql`
- Base PostgreSQL: schema `obrail_transport` (tables `station`, `vehicule`, `trajet`)
- API REST: `api/main.py` + doc `docs/api.md`
- Documentation technique: `docs/rapport_technique.md`
- Tableau de bord: `api/index.html` + `docs/tableau_de_bord.md`
- Soutenance: `docs/soutenance.md`

## Dossiers a fournir
- `docs/` (tous les documents)
- `data/scripts/` (ETL + SQL schema)
- `api/` (API et interface web)
