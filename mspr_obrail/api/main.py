"""
API REST ObRail Europe (FastAPI)
- Expose le data mart (PostgreSQL)
- Endpoints de consultation des trajets (jour/nuit, pays, opérateur)
"""

from __future__ import annotations

import os
from typing import Optional

import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "obrail_europe"),
        user=os.environ.get("PGUSER", "shindh"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def get_db():
    conn = _get_conn()
    try:
        yield conn
    finally:
        conn.close()


app = FastAPI(
    title="ObRail Europe API",
    version="1.0.0",
    description="API REST d'accès au data mart ferroviaire (jour/nuit, pays, opérateurs).",
)


@app.get("/", response_class=HTMLResponse)
def ui_home():
        """Interface web simple pour explorer les trajets."""
        return """
        <!doctype html>
        <html lang="fr">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>ObRail Europe – Explorer</title>
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 24px; background:#f6f7fb; }
                .card { background:#fff; padding:20px; border-radius:12px; box-shadow:0 2px 10px rgba(0,0,0,.08); }
                .map-card { margin-top:16px; }
                .grid { display:grid; gap:12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
                label { font-weight:600; font-size:14px; }
                input, select { width:100%; padding:10px; border:1px solid #ddd; border-radius:8px; }
                button { padding:12px 16px; border:0; border-radius:8px; background:#20603d; color:#fff; cursor:pointer; }
                table { width:100%; border-collapse: collapse; margin-top:16px; }
                th, td { text-align:left; padding:8px; border-bottom:1px solid #eee; font-size:13px; }
                .muted { color:#666; font-size:12px; }
                #map { width:100%; height:480px; }
            </style>
        </head>
        <body>
            <div class="card">
                <h2>ObRail Europe – Explorer les trajets</h2>
                <p class="muted">Filtres : pays, opérateur, gares de départ/arrivée, jour/nuit.</p>
                <div class="grid">
                    <div>
                        <label>Pays</label>
                        <select id="country">
                            <option value="">Tous</option>
                        </select>
                    </div>
                    <div>
                        <label>Opérateur (id)</label>
                        <input id="operator" placeholder="sncf_voyageurs" />
                    </div>
                    <div>
                        <label>Gare départ</label>
                        <input id="departure" placeholder="Paris" />
                    </div>
                    <div>
                        <label>Gare arrivée</label>
                        <input id="arrival" placeholder="Lyon" />
                    </div>
                    <div>
                        <label>Type</label>
                        <select id="night">
                            <option value="">Tous</option>
                            <option value="true">Nuit</option>
                            <option value="false">Jour</option>
                        </select>
                    </div>
                    <div>
                        <label>Limite</label>
                        <input id="limit" type="number" value="50" min="1" max="10000" />
                    </div>
                </div>
                <div style="margin-top:12px;">
                    <button onclick="loadTrips()">Rechercher</button>
                </div>
                <div id="result"></div>
                <div id="stops" class="muted"></div>
            </div>
            <div class="card map-card">
                <h3>Itinéraires (GPS)</h3>
                <p class="muted">Trajets affichés selon les filtres (lignes départ → arrivée).</p>
                <div id="map"></div>
            </div>
            <script>
                let map;
                let routeLayer;

                function initMap(){
                    map = L.map('map').setView([48.5, 10], 5);
                    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                        maxZoom: 18,
                        attribution: '&copy; OpenStreetMap contributors'
                    }).addTo(map);
                    routeLayer = L.layerGroup().addTo(map);
                }

                function resetRoutes(){
                    routeLayer.clearLayers();
                }

                function addRoute(depLat, depLon, arrLat, arrLon){
                    const line = L.polyline([[depLat, depLon], [arrLat, arrLon]], {
                        color: '#20603d',
                        weight: 2,
                        opacity: 0.7
                    });
                    line.addTo(routeLayer);
                }

                async function loadCountries(){
                    const res = await fetch('/countries');
                    const data = await res.json();
                    const select = document.getElementById('country');
                    for(const row of data){
                        const opt = document.createElement('option');
                        opt.value = row.country_code;
                        opt.textContent = `${row.country_code} - ${row.name_en || row.name_fr || ''}`;
                        select.appendChild(opt);
                    }
                }

                async function loadTrips(){
                    const params = new URLSearchParams();
                    const country = document.getElementById('country').value.trim();
                    const operator = document.getElementById('operator').value.trim();
                    const departure = document.getElementById('departure').value.trim();
                    const arrival = document.getElementById('arrival').value.trim();
                    const night = document.getElementById('night').value;
                    const limit = document.getElementById('limit').value || 50;

                    if(country) params.append('country_code', country);
                    if(operator) params.append('operator_id', operator);
                    if(departure) params.append('departure_station', departure);
                    if(arrival) params.append('arrival_station', arrival);
                    if(night) params.append('is_night', night);
                    params.append('limit', limit);

                    const res = await fetch('/trips?' + params.toString());
                    const data = await res.json();
                    const table = ['<table><thead><tr>',
                        '<th>ID</th><th>Pays</th><th>Opérateur</th><th>Type</th><th>Date départ</th><th>Heure départ</th><th>Station départ</th><th>GPS départ</th><th>Date arrivée</th><th>Heure arrivée</th><th>Station arrivée</th><th>GPS arrivée</th><th>Arrêts</th>',
                        '</tr></thead><tbody>'
                    ];
                    resetRoutes();
                    let bounds = [];
                    for(const row of data){
                        const depGps = row.departure_lat ? `${row.departure_lat}, ${row.departure_lon}` : '';
                        const arrGps = row.arrival_lat ? `${row.arrival_lat}, ${row.arrival_lon}` : '';
                        const typeLabel = row.is_night ? 'Nuit' : 'Jour';
                        table.push(`<tr><td>${row.fact_trip_key}</td><td>${row.country || ''}</td><td>${row.operator || ''}</td>` +
                            `<td>${typeLabel}</td><td>${row.departure_date || ''}</td><td>${row.departure_time || ''}</td>` +
                            `<td>${row.departure_station || ''}</td><td>${depGps}</td>` +
                            `<td>${row.arrival_date || ''}</td><td>${row.arrival_time || ''}</td>` +
                            `<td>${row.arrival_station || ''}</td><td>${arrGps}</td>` +
                            `<td><button onclick="loadStops('${row.trip_id}', '${row.operator}', '${row.country}')">Voir</button></td></tr>`);

                        if(row.departure_lat && row.departure_lon && row.arrival_lat && row.arrival_lon){
                            addRoute(row.departure_lat, row.departure_lon, row.arrival_lat, row.arrival_lon);
                            bounds.push([row.departure_lat, row.departure_lon]);
                            bounds.push([row.arrival_lat, row.arrival_lon]);
                        }
                    }
                    table.push('</tbody></table>');
                    document.getElementById('result').innerHTML = table.join('');

                    if(!data.length){
                        document.getElementById('stops').innerHTML = '';
                    }

                    if(bounds.length){
                        map.fitBounds(bounds, { padding: [20, 20] });
                    }
                }

                async function loadStops(tripId, operatorId, countryCode){
                    const params = new URLSearchParams();
                    params.append('trip_id', tripId);
                    params.append('operator_id', operatorId);
                    params.append('country_code', countryCode);
                    const res = await fetch('/trip_stops?' + params.toString());
                    const data = await res.json();
                    const lines = data.map(s =>
                        `${s.stop_sequence}. ${s.stop_name || s.stop_id} (${s.stop_lat}, ${s.stop_lon}) ` +
                        `Arr: ${s.arrival_time || ''} Dep: ${s.departure_time || ''}`
                    );
                    document.getElementById('stops').innerHTML =
                        '<strong>Arrêts intermédiaires:</strong><br>' + (lines.join('<br>') || 'Aucun arrêt trouvé');
                }

                loadCountries();
                initMap();
                loadTrips();
            </script>
        </body>
        </html>
        """


class TripSegment(BaseModel):
    fact_trip_key: int
    country: Optional[str]
    operator: Optional[str]
    trip_id: Optional[str]
    route_id: Optional[str]
    departure_station: Optional[str]
    arrival_station: Optional[str]
    departure_time: Optional[str]
    arrival_time: Optional[str]
    departure_date: Optional[str]
    arrival_date: Optional[str]
    departure_lat: Optional[float]
    departure_lon: Optional[float]
    arrival_lat: Optional[float]
    arrival_lon: Optional[float]
    is_night: Optional[bool]
    is_cross_border: Optional[bool]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/trips", response_model=list[TripSegment])
def get_trips(
    is_night: Optional[bool] = Query(default=None),
    country_code: Optional[str] = Query(default=None, min_length=2, max_length=64),
    operator_id: Optional[str] = Query(default=None, min_length=1, max_length=64),
    departure_station: Optional[str] = Query(default=None, min_length=2, max_length=128),
    arrival_station: Optional[str] = Query(default=None, min_length=2, max_length=128),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    """Retourne les trajets filtrés (jour/nuit, pays, opérateur, gares départ/arrivée)."""
    filters = []
    params = []

    if is_night is not None:
        filters.append("f.is_night = %s")
        params.append(is_night)
    if country_code:
        filters.append("c.country_code = %s")
        params.append(country_code.upper())
    if operator_id:
        filters.append("o.operator_id = %s")
        params.append(operator_id)
    if departure_station:
        filters.append("ds.station_name ILIKE %s")
        params.append(f"%{departure_station}%")
    if arrival_station:
        filters.append("a.station_name ILIKE %s")
        params.append(f"%{arrival_station}%")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
        SELECT
            f.fact_trip_key,
            c.country_code,
            o.operator_id,
            f.trip_business_id,
            r.route_id,
            ds.station_name AS departure_station,
            a.station_name AS arrival_station,
            ds.station_lat AS departure_lat,
            ds.station_lon AS departure_lon,
            a.station_lat AS arrival_lat,
            a.station_lon AS arrival_lon,
            dt.time_value::text AS departure_time,
            at.time_value::text AS arrival_time,
            d.date_value::text AS departure_date,
            d.date_value::text AS arrival_date,
            f.is_night,
            f.is_cross_border
        FROM obrail.fact_trip_segment f
        LEFT JOIN obrail.dim_country c ON c.country_key = f.country_key
        LEFT JOIN obrail.dim_operator o ON o.operator_key = f.operator_key
        LEFT JOIN obrail.dim_route r ON r.route_key = f.route_key
        LEFT JOIN obrail.dim_station ds ON ds.station_key = f.departure_station_key
        LEFT JOIN obrail.dim_station a ON a.station_key = f.arrival_station_key
        LEFT JOIN obrail.dim_time dt ON dt.time_key = f.departure_time_key
        LEFT JOIN obrail.dim_time at ON at.time_key = f.arrival_time_key
        LEFT JOIN obrail.dim_date d ON d.date_key = f.date_key
        {where_clause}
        ORDER BY f.fact_trip_key
        LIMIT %s OFFSET %s;
    """

    params.extend([limit, offset])

    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        TripSegment(
            fact_trip_key=row[0],
            country=row[1],
            operator=row[2],
            trip_id=row[3],
            route_id=row[4],
            departure_station=row[5],
            arrival_station=row[6],
            departure_time=row[11],
            arrival_time=row[12],
            departure_date=row[13],
            arrival_date=row[14],
            departure_lat=row[7],
            departure_lon=row[8],
            arrival_lat=row[9],
            arrival_lon=row[10],
            is_night=row[15],
            is_cross_border=row[16],
        )
        for row in rows
    ]


@app.get("/trip_stops")
def get_trip_stops(
    trip_id: str = Query(min_length=1, max_length=128),
    operator_id: str = Query(min_length=1, max_length=64),
    country_code: str = Query(min_length=2, max_length=64),
    db=Depends(get_db),
):
    sql = """
        SELECT
            stop_sequence,
            stop_id,
            stop_name,
            stop_lat,
            stop_lon,
            arrival_time::text,
            departure_time::text,
            date_value::text
        FROM obrail.trip_stop
        WHERE trip_id = %s AND operator_id = %s AND country_code = %s
        ORDER BY stop_sequence ASC;
    """

    with db.cursor() as cur:
        cur.execute(sql, (trip_id, operator_id, country_code))
        rows = cur.fetchall()

    return [
        {
            "stop_sequence": row[0],
            "stop_id": row[1],
            "stop_name": row[2],
            "stop_lat": row[3],
            "stop_lon": row[4],
            "arrival_time": row[5],
            "departure_time": row[6],
            "date_value": row[7],
        }
        for row in rows
    ]


@app.get("/coverage")
def get_coverage(db=Depends(get_db)):
    sql = """
        SELECT
            c.country_code,
            c.iso3_code,
            c.country_name_fr,
            c.country_name_en,
            COALESCE(COUNT(f.fact_trip_key), 0) AS trips
        FROM obrail.dim_country c
        LEFT JOIN obrail.fact_trip_segment f ON f.country_key = c.country_key
        WHERE (c.eu_member='T' OR c.efta_member='T' OR c.candidate_member='T')
        GROUP BY c.country_code, c.iso3_code, c.country_name_fr, c.country_name_en
        ORDER BY trips DESC;
    """

    with db.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [
        {
            "country_code": row[0],
            "iso3_code": row[1],
            "name_fr": row[2],
            "name_en": row[3],
            "trips": row[4],
        }
        for row in rows
    ]


@app.get("/countries")
def list_countries(db=Depends(get_db)):
    sql = """
        SELECT DISTINCT c.country_code, c.country_name_fr, c.country_name_en
        FROM obrail.dim_country c
        INNER JOIN obrail.fact_trip_segment f ON f.country_key = c.country_key
        ORDER BY c.country_name_en;
    """
    with db.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [
        {"country_code": row[0], "name_fr": row[1], "name_en": row[2]}
        for row in rows
    ]


@app.get("/operators")
def list_operators(db=Depends(get_db)):
    sql = """
        SELECT operator_id, operator_name, operator_country, is_night_operator
        FROM obrail.dim_operator
        ORDER BY operator_name;
    """
    with db.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [
        {
            "operator_id": row[0],
            "operator_name": row[1],
            "operator_country": row[2],
            "is_night_operator": row[3],
        }
        for row in rows
    ]


@app.get("/stats/coverage")
def coverage_stats(db=Depends(get_db)):
    sql = """
        SELECT
            COUNT(*) AS total_trips,
            SUM(CASE WHEN is_night THEN 1 ELSE 0 END) AS night_trips,
            SUM(CASE WHEN is_cross_border THEN 1 ELSE 0 END) AS cross_border_trips
        FROM obrail.fact_trip_segment;
    """
    with db.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No data")

    total = row[0] or 0
    night = row[1] or 0
    cross = row[2] or 0

    return {
        "total_trips": total,
        "night_trips": night,
        "night_ratio": (night / total) if total else 0,
        "cross_border_trips": cross,
    }
