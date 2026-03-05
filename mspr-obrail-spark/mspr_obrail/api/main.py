"""
ObRail Transport API
Expose data from the transport ETL (vehicule, station, trajet).
"""

from __future__ import annotations

import os
import csv
from functools import lru_cache
from pathlib import Path
from typing import Optional

import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

STATIC_DIR = Path(__file__).parent
SCHEMA = os.environ.get("PGSCHEMA", "obrail_transport")
CATALOG_PATH = (
    os.environ.get("GTFS_CATALOG_URL")
    or os.environ.get("MOBILITY_DATABASE_CATALOG_PATH")
    or os.environ.get("MOBILITY_DATABASE_CATALOG_URL")
)

COUNTRY_CODE_NAMES: dict[str, str] = {
    "FR": "France",
    "GB": "Great Britain",
    "UK": "Great Britain",
    "DE": "Germany",
    "FI": "Finland",
    "CZ": "Czechia",
    "ES": "Spain",
    "IT": "Italy",
    "NL": "Netherlands",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "PL": "Poland",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "PT": "Portugal",
    "IE": "Ireland",
    "LU": "Luxembourg",
    "GR": "Greece",
    "RO": "Romania",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "HU": "Hungary",
    "LT": "Lithuania",
    "LV": "Latvia",
    "EE": "Estonia",
}


def _normalize_country_name(value: str) -> str:
    text = value.strip()
    if not text:
        return text
    lowered = text.lower()
    if lowered in ("nan", "null", "none"):
        return ""
    if text.isupper() or text.islower():
        text = text.replace("-", " ").replace("_", " ").title()
    return text


@lru_cache(maxsize=1)
def _country_mapping() -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not CATALOG_PATH:
        return mapping
    if CATALOG_PATH.startswith("http://") or CATALOG_PATH.startswith("https://"):
        return mapping
    if not os.path.exists(CATALOG_PATH):
        return mapping
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                code = (
                    row.get("country_code")
                    or row.get("location.country_code")
                    or row.get("countryCode")
                )
                name = (
                    row.get("country")
                    or row.get("location.country")
                    or row.get("country_name")
                    or row.get("country_name_en")
                    or row.get("country_name_fr")
                    or row.get("countryName")
                )
                if not code:
                    continue
                code = code.strip().upper()
                if not code:
                    continue
                if name:
                    name = _normalize_country_name(name)
                if name:
                    mapping.setdefault(code, name)
    except Exception:
        return mapping
    for code, name in COUNTRY_CODE_NAMES.items():
        mapping.setdefault(code.upper(), name)
    return mapping


def _country_name(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    return _country_mapping().get(code.upper(), code.upper())


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "obrail"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def get_db():
    conn = _get_conn()
    try:
        yield conn
    finally:
        conn.close()


app = FastAPI(
    title="ObRail Transport API",
    version="1.0.0",
    description="REST API exposing transport data (train/plane) from the Spark ETL.",
)


@app.get("/", response_class=FileResponse, include_in_schema=False)
def ui_home():
    path = STATIC_DIR / "index.html"
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"index.html not found - expected at {path.resolve()}",
        )
    return FileResponse(path, media_type="text/html")


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "version": app.version}


class Vehicule(BaseModel):
    vehicule_id: int
    type_transport: str
    specificite: Optional[str]
    train_type: Optional[int]


class Station(BaseModel):
    station_id: int
    station_key: int
    stop_id: Optional[str] = None
    station_name: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    pays: Optional[str]
    station_lat: Optional[float] = None
    station_lon: Optional[float] = None
    country_code: Optional[str] = None
    country_name: Optional[str] = None


class Trajet(BaseModel):
    trajet_id: int
    vehicule_id: int
    type_transport: Optional[str]
    specificite: Optional[str]
    train_type: Optional[int]
    is_night: Optional[bool]
    departure_station_id: int
    departure_station_name: Optional[str]
    departure_latitude: Optional[float]
    departure_longitude: Optional[float]
    departure_pays: Optional[str]
    arrival_station_id: int
    arrival_station_name: Optional[str]
    arrival_latitude: Optional[float]
    arrival_longitude: Optional[float]
    arrival_pays: Optional[str]
    distance_km: Optional[float]
    co2_kg: Optional[float]


class Stats(BaseModel):
    total_trajets: int
    total_vehicules: int
    total_stations: int
    night_trajets: int


@app.get("/countries", tags=["referential"])
def list_countries(db=Depends(get_db)):
    sql = f"""
        SELECT DISTINCT pays
        FROM {SCHEMA}.station
        WHERE pays IS NOT NULL AND pays <> ''
        ORDER BY pays;
    """
    with db.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [
        {
            "country_code": r[0],
            "name_fr": _country_name(r[0]),
            "name_en": _country_name(r[0]),
        }
        for r in rows
    ]


@app.get("/operators", tags=["referential"])
def list_operators(
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []
    if type_transport:
        filters.append("type_transport = %s")
        params.append(type_transport)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT
            COALESCE(specificite, type_transport) AS operator_id,
            COALESCE(specificite, type_transport) AS operator_name,
            NULL::text AS operator_country,
            (MAX(CASE WHEN train_type = 105 THEN 1 ELSE 0 END) = 1) AS is_night_operator
        FROM {SCHEMA}.vehicule
        {where}
        GROUP BY COALESCE(specificite, type_transport)
        ORDER BY operator_name;
    """
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [
        {
            "operator_id": r[0],
            "operator_name": r[1],
            "operator_country": r[2],
            "is_night_operator": r[3],
        }
        for r in rows
    ]


@app.get("/trips", tags=["trips"])
def list_trips_legacy(
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    is_night: Optional[bool] = Query(default=None),
    country_code: Optional[str] = Query(default=None, min_length=2, max_length=8),
    operator_id: Optional[str] = Query(default=None, min_length=1, max_length=128),
    train_kind: Optional[str] = Query(default=None, min_length=2, max_length=32),
    departure_station: Optional[str] = Query(default=None, min_length=2, max_length=256),
    arrival_station: Optional[str] = Query(default=None, min_length=2, max_length=256),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []

    if type_transport:
        filters.append("v.type_transport = %s")
        params.append(type_transport)
    if is_night is not None:
        filters.append("t.is_night = %s")
        params.append(is_night)
    if country_code:
        filters.append("(ds.pays = %s OR a.pays = %s)")
        params.extend([country_code.upper(), country_code.upper()])
    if operator_id:
        filters.append("COALESCE(v.specificite, v.type_transport) = %s")
        params.append(operator_id)
    if train_kind:
        kind = train_kind.lower().strip()
        patterns: list[str] | None = None
        if kind == "tgv":
            patterns = ["%tgv%", "%inoui%"]
        elif kind == "rer":
            patterns = ["%rer%"]
        elif kind in ("intercite", "intercité", "intercity"):
            patterns = ["%intercite%", "%intercité%", "%intercity%"]
        if patterns:
            filters.append(
                "v.type_transport = 'train' AND (" + " OR ".join(["v.specificite ILIKE %s"] * len(patterns)) + ")"
            )
            params.extend(patterns)
    if departure_station:
        filters.append("ds.station_name ILIKE %s")
        params.append(f"%{departure_station}%")
    if arrival_station:
        filters.append("a.station_name ILIKE %s")
        params.append(f"%{arrival_station}%")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT
            t.trajet_id AS fact_trip_key,
            COALESCE(ds.pays, a.pays) AS country,
            COALESCE(v.specificite, v.type_transport) AS operator,
            v.type_transport AS type_transport,
            v.specificite AS specificite,
            t.trajet_id::text AS trip_id,
            NULL::text AS route_id,
            ds.station_name AS departure_station,
            a.station_name AS arrival_station,
            ds.latitude AS departure_lat,
            ds.longitude AS departure_lon,
            a.latitude AS arrival_lat,
            a.longitude AS arrival_lon,
            t.distance_km AS distance_km,
            t.co2_kg AS co2_kg,
            NULL::text AS departure_time,
            NULL::text AS arrival_time,
            NULL::text AS departure_date,
            NULL::text AS arrival_date,
            t.is_night AS is_night,
            CASE
                WHEN ds.pays IS NOT NULL AND a.pays IS NOT NULL AND ds.pays <> a.pays THEN TRUE
                ELSE FALSE
            END AS is_cross_border
        FROM {SCHEMA}.trajet t
        LEFT JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
        LEFT JOIN {SCHEMA}.station ds ON ds.station_id = t.departure_station_id
        LEFT JOIN {SCHEMA}.station a ON a.station_id = t.arrival_station_id
        {where}
        ORDER BY t.trajet_id
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        {
            "fact_trip_key": r[0],
            "country": r[1],
            "operator": r[2],
            "type_transport": r[3],
            "specificite": r[4],
            "trip_id": r[5],
            "route_id": r[6],
            "departure_station": r[7],
            "arrival_station": r[8],
            "departure_lat": r[9],
            "departure_lon": r[10],
            "arrival_lat": r[11],
            "arrival_lon": r[12],
            "distance_km": r[13],
            "co2_kg": r[14],
            "departure_time": r[15],
            "arrival_time": r[16],
            "departure_date": r[17],
            "arrival_date": r[18],
            "is_night": r[19],
            "is_cross_border": r[20],
        }
        for r in rows
    ]


@app.get("/coverage", tags=["stats"])
def coverage_legacy(
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []
    if type_transport:
        filters.append("v.type_transport = %s")
        params.append(type_transport)
    where = f"AND {' AND '.join(filters)}" if filters else ""
    sql = f"""
        WITH trip_countries AS (
            SELECT t.trajet_id, ds.pays AS country_code
            FROM {SCHEMA}.trajet t
            LEFT JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
            LEFT JOIN {SCHEMA}.station ds ON ds.station_id = t.departure_station_id
            WHERE ds.pays IS NOT NULL AND ds.pays <> '' {where}
            UNION
            SELECT t.trajet_id, a.pays AS country_code
            FROM {SCHEMA}.trajet t
            LEFT JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
            LEFT JOIN {SCHEMA}.station a ON a.station_id = t.arrival_station_id
            WHERE a.pays IS NOT NULL AND a.pays <> '' {where}
        )
        SELECT
            country_code,
            NULL::text AS iso3_code,
            country_code AS name_fr,
            country_code AS name_en,
            COUNT(DISTINCT trajet_id) AS trips
        FROM trip_countries
        GROUP BY country_code
        ORDER BY trips DESC;
    """
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [
        {
            "country_code": r[0],
            "iso3_code": r[1],
            "name_fr": _country_name(r[0]),
            "name_en": _country_name(r[0]),
            "trips": r[4],
        }
        for r in rows
    ]


@app.get("/stats/coverage", tags=["stats"])
def coverage_stats_legacy(
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []
    if type_transport:
        filters.append("v.type_transport = %s")
        params.append(type_transport)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT
            COUNT(*) AS total_trips,
            SUM(CASE WHEN t.is_night THEN 1 ELSE 0 END) AS night_trips,
            SUM(
                CASE
                    WHEN ds.pays IS NOT NULL AND a.pays IS NOT NULL AND ds.pays <> a.pays THEN 1
                    ELSE 0
                END
            ) AS cross_border_trips
        FROM {SCHEMA}.trajet t
        LEFT JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
        LEFT JOIN {SCHEMA}.station ds ON ds.station_id = t.departure_station_id
        LEFT JOIN {SCHEMA}.station a ON a.station_id = t.arrival_station_id
        {where};
    """
    with db.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No data found in transport schema.")

    total = row[0] or 0
    night = row[1] or 0
    cross = row[2] or 0
    return {
        "total_trips": total,
        "night_trips": night,
        "night_ratio": round(night / total, 4) if total else 0,
        "cross_border_trips": cross,
        "cross_border_ratio": round(cross / total, 4) if total else 0,
    }


@app.get("/vehicules", response_model=list[Vehicule], tags=["referential"])
def list_vehicules(
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    specificite: Optional[str] = Query(default=None, min_length=1, max_length=128),
    train_type: Optional[int] = Query(default=None, ge=0),
    limit: int = Query(default=5000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []
    if type_transport:
        filters.append("type_transport = %s")
        params.append(type_transport)
    if specificite:
        filters.append("specificite ILIKE %s")
        params.append(f"%{specificite}%")
    if train_type is not None:
        filters.append("train_type = %s")
        params.append(train_type)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT vehicule_id, type_transport, specificite, train_type
        FROM {SCHEMA}.vehicule
        {where}
        ORDER BY vehicule_id
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [
        Vehicule(
            vehicule_id=r[0],
            type_transport=r[1],
            specificite=r[2],
            train_type=r[3],
        )
        for r in rows
    ]


@app.get("/stations", response_model=list[Station], tags=["referential"])
def list_stations(
    pays: Optional[str] = Query(default=None, min_length=2, max_length=8),
    country_code: Optional[str] = Query(default=None, min_length=2, max_length=8),
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    name: Optional[str] = Query(default=None, min_length=1, max_length=256),
    limit: int = Query(default=5000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []
    country = (country_code or pays)
    if country:
        filters.append("pays = %s")
        params.append(country.upper())
    if name:
        filters.append("station_name ILIKE %s")
        params.append(f"%{name}%")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    if type_transport:
        params.insert(0, type_transport)
        station_filter = f"""
            WITH station_ids AS (
                SELECT t.departure_station_id AS station_id
                FROM {SCHEMA}.trajet t
                JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
                WHERE v.type_transport = %s
                UNION
                SELECT t.arrival_station_id AS station_id
                FROM {SCHEMA}.trajet t
                JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
                WHERE v.type_transport = %s
            )
        """
        params.insert(1, type_transport)
        sql_with_stop_id = f"""
            {station_filter}
            SELECT s.station_id, s.stop_id, s.station_name, s.latitude, s.longitude, s.pays
            FROM {SCHEMA}.station s
            JOIN station_ids f ON f.station_id = s.station_id
            {where}
            ORDER BY s.station_name
            LIMIT %s OFFSET %s;
        """
        sql_without_stop_id = f"""
            {station_filter}
            SELECT s.station_id, s.station_name, s.latitude, s.longitude, s.pays
            FROM {SCHEMA}.station s
            JOIN station_ids f ON f.station_id = s.station_id
            {where}
            ORDER BY s.station_name
            LIMIT %s OFFSET %s;
        """
    else:
        sql_with_stop_id = f"""
            SELECT station_id, stop_id, station_name, latitude, longitude, pays
            FROM {SCHEMA}.station
            {where}
            ORDER BY station_name
            LIMIT %s OFFSET %s;
        """
        sql_without_stop_id = f"""
            SELECT station_id, station_name, latitude, longitude, pays
            FROM {SCHEMA}.station
            {where}
            ORDER BY station_name
            LIMIT %s OFFSET %s;
        """
    params.extend([limit, offset])
    with db.cursor() as cur:
        try:
            cur.execute(sql_with_stop_id, params)
            rows = cur.fetchall()
            return [
                Station(
                    station_id=r[0],
                    station_key=r[0],
                    stop_id=r[1],
                    station_name=r[2],
                    latitude=r[3],
                    longitude=r[4],
                    pays=r[5],
                    station_lat=r[3],
                    station_lon=r[4],
                    country_code=r[5],
                    country_name=_country_name(r[5]),
                )
                for r in rows
            ]
        except psycopg2.errors.UndefinedColumn:
            cur.execute(sql_without_stop_id, params)
            rows = cur.fetchall()
            return [
                Station(
                    station_id=r[0],
                    station_key=r[0],
                    stop_id=None,
                    station_name=r[1],
                    latitude=r[2],
                    longitude=r[3],
                    pays=r[4],
                    station_lat=r[2],
                    station_lon=r[3],
                    country_code=r[4],
                    country_name=_country_name(r[4]),
                )
                for r in rows
            ]


@app.get("/trajets", response_model=list[Trajet], tags=["trips"])
def list_trajets(
    type_transport: Optional[str] = Query(default=None, min_length=2, max_length=32),
    specificite: Optional[str] = Query(default=None, min_length=1, max_length=128),
    train_type: Optional[int] = Query(default=None, ge=0),
    is_night: Optional[bool] = Query(default=None),
    pays: Optional[str] = Query(default=None, min_length=2, max_length=8),
    departure_station: Optional[str] = Query(default=None, min_length=2, max_length=256),
    arrival_station: Optional[str] = Query(default=None, min_length=2, max_length=256),
    min_distance_km: Optional[float] = Query(default=None, ge=0),
    max_distance_km: Optional[float] = Query(default=None, ge=0),
    limit: int = Query(default=1000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    filters: list[str] = []
    params: list = []

    if type_transport:
        filters.append("v.type_transport = %s")
        params.append(type_transport)
    if specificite:
        filters.append("v.specificite ILIKE %s")
        params.append(f"%{specificite}%")
    if train_type is not None:
        filters.append("v.train_type = %s")
        params.append(train_type)
    if is_night is not None:
        filters.append("t.is_night = %s")
        params.append(is_night)
    if pays:
        filters.append("(ds.pays = %s OR a.pays = %s)")
        params.extend([pays.upper(), pays.upper()])
    if departure_station:
        filters.append("ds.station_name ILIKE %s")
        params.append(f"%{departure_station}%")
    if arrival_station:
        filters.append("a.station_name ILIKE %s")
        params.append(f"%{arrival_station}%")
    if min_distance_km is not None:
        filters.append("t.distance_km >= %s")
        params.append(min_distance_km)
    if max_distance_km is not None:
        filters.append("t.distance_km <= %s")
        params.append(max_distance_km)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT
            t.trajet_id,
            t.vehicule_id,
            v.type_transport,
            v.specificite,
            v.train_type,
            t.is_night,
            t.departure_station_id,
            ds.station_name,
            ds.latitude,
            ds.longitude,
            ds.pays,
            t.arrival_station_id,
            a.station_name,
            a.latitude,
            a.longitude,
            a.pays,
            t.distance_km,
            t.co2_kg
        FROM {SCHEMA}.trajet t
        LEFT JOIN {SCHEMA}.vehicule v ON v.vehicule_id = t.vehicule_id
        LEFT JOIN {SCHEMA}.station ds ON ds.station_id = t.departure_station_id
        LEFT JOIN {SCHEMA}.station a ON a.station_id = t.arrival_station_id
        {where}
        ORDER BY t.trajet_id
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        Trajet(
            trajet_id=r[0],
            vehicule_id=r[1],
            type_transport=r[2],
            specificite=r[3],
            train_type=r[4],
            is_night=r[5],
            departure_station_id=r[6],
            departure_station_name=r[7],
            departure_latitude=r[8],
            departure_longitude=r[9],
            departure_pays=r[10],
            arrival_station_id=r[11],
            arrival_station_name=r[12],
            arrival_latitude=r[13],
            arrival_longitude=r[14],
            arrival_pays=r[15],
            distance_km=r[16],
            co2_kg=r[17],
        )
        for r in rows
    ]


@app.get("/stats", response_model=Stats, tags=["stats"])
def stats(db=Depends(get_db)):
    sql = f"""
        SELECT
            (SELECT COUNT(*) FROM {SCHEMA}.trajet)   AS total_trajets,
            (SELECT COUNT(*) FROM {SCHEMA}.vehicule) AS total_vehicules,
            (SELECT COUNT(*) FROM {SCHEMA}.station)  AS total_stations,
            (SELECT COUNT(*) FROM {SCHEMA}.trajet WHERE is_night) AS night_trajets;
    """
    with db.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No data found in transport schema.")
    return Stats(
        total_trajets=row[0] or 0,
        total_vehicules=row[1] or 0,
        total_stations=row[2] or 0,
        night_trajets=row[3] or 0,
    )
