"""
ETL en streaming (sans stockage local):
- Télécharge les GTFS et CSV en mémoire
- Transforme en DataFrames
- Construit le data mart
- Charge directement PostgreSQL
"""

from __future__ import annotations

import io
import logging
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import requests

LOGGER = logging.getLogger("stream_etl")

MOBILITY_DATABASE_CATALOG_URL = "https://files.mobilitydatabase.org/feeds_v2.csv"

GTFS_SOURCES = [
    {
        "source": "SNCF Open Data (Horaires SNCF, GTFS)",
        "country": "france",
        "operator": "sncf_voyageurs",
        "url": "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip",
    },
    {
        "source": "HSL (Helsinki Region Transport, Open Data)",
        "country": "finland",
        "operator": "hsl",
        "url": "https://infopalvelut.storage.hsldev.com/gtfs/hsl.zip",
    },
    {
        "source": "GTFS.DE (Schienenfernverkehr)",
        "country": "germany",
        "operator": "deutsche_bahn_fv",
        "url": "https://download.gtfs.de/germany/fv_free/latest.zip",
    },
    {
        "source": "GTFS.DE (Schienenregionalverkehr)",
        "country": "germany",
        "operator": "regionalverkehr",
        "url": "https://download.gtfs.de/germany/rv_free/latest.zip",
    },
    {
        "source": "Prague Integrated Transport (PID Open Data)",
        "country": "czechia",
        "operator": "pid",
        "url": "https://data.pid.cz/PID_GTFS.zip",
    },
]

NIGHT_TRAINS_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY/"
    "export?format=csv"
)

GEO_URL = "https://gisco-services.ec.europa.eu/distribution/v2/countries/csv/CNTR_AT_2024.csv"

CRITICAL_COLUMNS = [
    "departure_stop_id",
    "arrival_stop_id",
    "departure_time",
    "arrival_time",
]

COUNTRY_ALIASES = {
    "FR": "france",
    "GB": "gb",
    "CZ": "czechia",
    "DE": "germany",
    "FI": "finland",
}

DEFAULT_TARGET_COUNTRIES = {
    "FR",
    "DE",
    "GB",
    "IT",
    "ES",
    "NL",
    "BE",
    "CH",
    "AT",
    "PL",
    "SE",
    "NO",
    "DK",
}


def _setup_logger() -> None:
    if LOGGER.handlers:
        return
    LOGGER.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


def _parse_country_codes(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip().upper() for item in value.split(",") if item.strip()}


def _match_country(target_codes: set[str], country_value: str | None) -> bool:
    if not target_codes or not country_value:
        return True
    country_value = str(country_value).strip().lower()
    if country_value.upper() in target_codes:
        return True
    for code, alias in COUNTRY_ALIASES.items():
        if code in target_codes and alias == country_value:
            return True
    return False


def _download_bytes(url: str) -> bytes | None:
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        LOGGER.warning("Téléchargement impossible: %s (%s)", url, exc)
        return None


def _read_zip_csv(zip_file: zipfile.ZipFile, filename: str, usecols: list[str]) -> pd.DataFrame:
    try:
        with zip_file.open(filename) as handle:
            header_df = pd.read_csv(handle, nrows=0)
        available = [col for col in usecols if col in header_df.columns]
        cols = available if available else None
        with zip_file.open(filename) as handle:
            return pd.read_csv(handle, dtype=str, low_memory=False, usecols=cols, on_bad_lines="skip")
    except KeyError:
        return pd.DataFrame()


def _normalize_country(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _normalize_key(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _normalize_station_name(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


def _normalize_time(value: str) -> str:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) < 2:
        return text
    hour = int(parts[0]) % 24
    return f"{hour:02d}:{parts[1]}:{parts[2] if len(parts) > 2 else '00'}"


def _is_night_time(time_value: str) -> bool:
    if not time_value:
        return False
    text = str(time_value).strip().lower()
    if not text or text == "nan":
        return False
    try:
        hour = int(text.split(":")[0]) % 24
    except ValueError:
        return False
    return hour >= 20 or hour < 6


def _build_service_date_map(
    calendar_df: pd.DataFrame,
    calendar_dates_df: pd.DataFrame,
) -> dict[str, str]:
    service_date_map: dict[str, str] = {}

    if not calendar_dates_df.empty and "exception_type" in calendar_dates_df.columns:
        added = calendar_dates_df[
            calendar_dates_df["exception_type"].astype(str) == "1"
        ]
        if not added.empty:
            added_dates = (
                added.groupby("service_id")["date"].min().dropna().to_dict()
            )
            for service_id, date_value in added_dates.items():
                service_date_map[str(service_id)] = str(date_value)

    if not calendar_df.empty:
        calendar_dates = (
            calendar_df.groupby("service_id")["start_date"].min().dropna().to_dict()
        )
        for service_id, date_value in calendar_dates.items():
            service_date_map.setdefault(str(service_id), str(date_value))

    return service_date_map


def _is_cross_border_by_stops(
    stops_df: pd.DataFrame,
    departure_stop_id: str,
    arrival_stop_id: str,
) -> bool:
    if stops_df.empty:
        return False
    if "stop_country" not in stops_df.columns:
        return False
    dep = stops_df.loc[stops_df["stop_id"] == departure_stop_id, "stop_country"]
    arr = stops_df.loc[stops_df["stop_id"] == arrival_stop_id, "stop_country"]
    if dep.empty or arr.empty:
        return False
    return _normalize_country(dep.iloc[0]) != _normalize_country(arr.iloc[0])


def _score_rail_candidate(row: pd.Series) -> int:
    name = str(row.get("name", "")).lower()
    provider = str(row.get("provider", "")).lower()
    note = str(row.get("note", "")).lower()
    keywords = ["rail", "railway", "train", "railways", "national", "sncf", "db", "tgv", "ic", "intercity"]
    score = sum(1 for kw in keywords if kw in name or kw in provider or kw in note)
    if str(row.get("location.municipality", "")) in ("", "nan"):
        score += 1
    if str(row.get("is_official", "")).lower() in ("true", "t", "1"):
        score += 2
    return score


def get_mobility_database_sources(
    country_codes: set[str],
    max_per_country: int = 1,
) -> list[dict]:
    if not country_codes:
        return []

    try:
        df = pd.read_csv(
            MOBILITY_DATABASE_CATALOG_URL,
            usecols=[
                "id",
                "data_type",
                "location.country_code",
                "location.municipality",
                "provider",
                "is_official",
                "name",
                "note",
                "status",
                "urls.latest",
                "urls.direct_download",
            ],
            low_memory=False,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Catalogue Mobility Database indisponible: %s", exc)
        return []

    df = df[df["data_type"] == "gtfs"]
    df = df[df["status"].astype(str).str.lower() == "active"]
    df = df[df["location.country_code"].isin(country_codes)]

    df = df.copy()
    df["download_url"] = df["urls.latest"].fillna(df["urls.direct_download"])
    df = df[df["download_url"].notna()]
    if df.empty:
        return []

    df["score"] = df.apply(_score_rail_candidate, axis=1)

    sources = []
    for country_code, group in df.groupby("location.country_code"):
        official_group = group[group["is_official"].astype(str).str.lower().isin(["true", "t", "1"])]
        candidate_group = official_group if not official_group.empty else group
        candidate_group = candidate_group.sort_values("score", ascending=False)
        selected = candidate_group.head(max_per_country)
        for _, row in selected.iterrows():
            sources.append(
                {
                    "source": "Mobility Database",
                    "country": str(country_code).lower(),
                    "operator": str(row.get("id")),
                    "url": str(row.get("download_url")),
                }
            )
    return sources


def transform_trip_segments(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.drop_duplicates(subset=CRITICAL_COLUMNS + ["operator", "route_id"]).copy()
    if "service_date" in df.columns:
        df["service_date"] = df["service_date"].astype(str).str.strip()
    df["departure_time"] = df["departure_time"].replace("", pd.NA)
    df["arrival_time"] = df["arrival_time"].replace("", pd.NA)
    df["departure_time"] = df["departure_time"].fillna(df["arrival_time"])
    df["is_night"] = df["departure_time"].apply(_is_night_time)
    df = df.dropna(subset=CRITICAL_COLUMNS)
    df["load_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def transform_night_trains(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    keep_cols = [
        col
        for col in ["agency_id", "agency_name", "agency_state", "agency_url"]
        if col in df.columns
    ]
    df = df[keep_cols].copy()
    df = df.rename(
        columns={
            "agency_id": "operator_id",
            "agency_name": "operator_name",
            "agency_state": "operator_country",
            "agency_url": "operator_url",
        }
    )
    df["operator_country"] = df["operator_country"].apply(_normalize_country)
    df["is_night"] = True
    df = df.drop_duplicates()
    df["load_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def _build_country_mapping(dim_country: pd.DataFrame) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if dim_country.empty:
        return mapping

    for _, row in dim_country.iterrows():
        code = str(row.get("country_code", "")).strip().upper()
        if not code:
            continue
        mapping[_normalize_key(code)] = code

        for col in ["country_name_en", "country_name_fr", "iso3_code"]:
            value = row.get(col)
            if value:
                mapping[_normalize_key(value)] = code

    return mapping


def _map_country(value: str, mapping: dict[str, str]) -> str:
    if value is None:
        return ""
    key = _normalize_key(value)
    return mapping.get(key, _normalize_country(value))


def _build_dim_country(geo_df: pd.DataFrame) -> pd.DataFrame:
    if geo_df.empty:
        return pd.DataFrame()

    geo_df = geo_df.copy()
    geo_df["CNTR_ID"] = geo_df["CNTR_ID"].astype(str).str.upper()

    dim = geo_df[
        [
            "CNTR_ID",
            "NAME_ENGL",
            "NAME_FREN",
            "ISO3_CODE",
            "EU_STAT",
            "EFTA_STAT",
            "CC_STAT",
        ]
    ].dropna(subset=["CNTR_ID"])

    dim = dim.rename(
        columns={
            "CNTR_ID": "country_code",
            "NAME_ENGL": "country_name_en",
            "NAME_FREN": "country_name_fr",
            "ISO3_CODE": "iso3_code",
            "EU_STAT": "eu_member",
            "EFTA_STAT": "efta_member",
            "CC_STAT": "candidate_member",
        }
    ).drop_duplicates()

    dim = dim.reset_index(drop=True)
    dim.insert(0, "country_key", dim.index + 1)
    return dim


def _build_dim_operator(
    segments_df: pd.DataFrame,
    night_df: pd.DataFrame,
    country_mapping: dict[str, str],
) -> pd.DataFrame:
    operators_segments = segments_df[["operator", "country"]].copy()
    operators_segments = operators_segments.rename(
        columns={"operator": "operator_id", "country": "operator_country"}
    )
    operators_segments["operator_id"] = operators_segments["operator_id"].astype(str)
    operators_segments["operator_country"] = operators_segments["operator_country"].apply(
        lambda value: _map_country(value, country_mapping)
    )
    operators_segments["operator_name"] = operators_segments["operator_id"]
    operators_segments["is_night_operator"] = False

    operators_night = night_df.copy()
    if not operators_night.empty:
        operators_night = operators_night.rename(
            columns={
                "operator_id": "operator_id",
                "operator_name": "operator_name",
                "operator_country": "operator_country",
            }
        )
        operators_night["operator_country"] = operators_night["operator_country"].apply(
            lambda value: _map_country(value, country_mapping)
        )
        operators_night["is_night_operator"] = True

    operators = pd.concat([operators_segments, operators_night], ignore_index=True)
    operators = operators.dropna(subset=["operator_id"]).copy()

    grouped = operators.groupby("operator_id", as_index=False).agg(
        {
            "operator_name": "first",
            "operator_country": "first",
            "is_night_operator": "max",
        }
    )

    grouped = grouped.reset_index(drop=True)
    grouped.insert(0, "operator_key", grouped.index + 1)
    return grouped


def _build_dim_station(
    segments_df: pd.DataFrame,
    country_mapping: dict[str, str],
) -> pd.DataFrame:
    dep = segments_df[
        [
            "departure_stop_id",
            "departure_station",
            "departure_lat",
            "departure_lon",
            "country",
        ]
    ].copy()
    dep = dep.rename(
        columns={
            "departure_stop_id": "stop_id",
            "departure_station": "station_name",
            "departure_lat": "station_lat",
            "departure_lon": "station_lon",
            "country": "country_code",
        }
    )

    arr = segments_df[
        [
            "arrival_stop_id",
            "arrival_station",
            "arrival_lat",
            "arrival_lon",
            "country",
        ]
    ].copy()
    arr = arr.rename(
        columns={
            "arrival_stop_id": "stop_id",
            "arrival_station": "station_name",
            "arrival_lat": "station_lat",
            "arrival_lon": "station_lon",
            "country": "country_code",
        }
    )

    stations = pd.concat([dep, arr], ignore_index=True)
    stations = stations.dropna(subset=["stop_id"]).drop_duplicates()
    stations["country_code"] = stations["country_code"].apply(
        lambda value: _map_country(value, country_mapping)
    )

    stations = stations.reset_index(drop=True)
    stations.insert(0, "station_key", stations.index + 1)
    return stations


def _build_dim_route(
    segments_df: pd.DataFrame,
    country_mapping: dict[str, str],
) -> pd.DataFrame:
    routes = segments_df[["route_id", "operator", "country"]].copy()
    routes = routes.rename(
        columns={"operator": "operator_id", "country": "country_code"}
    )
    routes["country_code"] = routes["country_code"].apply(
        lambda value: _map_country(value, country_mapping)
    )
    routes = routes.dropna(subset=["route_id", "operator_id"]).drop_duplicates()
    routes = routes.reset_index(drop=True)
    routes.insert(0, "route_key", routes.index + 1)
    return routes


def _build_dim_time(segments_df: pd.DataFrame) -> pd.DataFrame:
    times = pd.concat(
        [segments_df["departure_time"], segments_df["arrival_time"]],
        ignore_index=True,
    ).dropna()

    times = times.astype(str)
    times = times[~times.str.lower().isin(["", "na", "nan", "none"])]

    times = times.drop_duplicates().reset_index(drop=True)
    dim = pd.DataFrame({"time_value": times})
    parts = dim["time_value"].str.split(":", expand=True)
    dim["hour"] = pd.to_numeric(parts[0], errors="coerce")
    dim["minute"] = pd.to_numeric(parts[1], errors="coerce")
    dim["second"] = pd.to_numeric(parts[2].fillna("0"), errors="coerce")
    dim = dim.dropna(subset=["hour", "minute", "second"]).copy()
    dim["hour"] = dim["hour"].astype(int)
    dim["minute"] = dim["minute"].astype(int)
    dim["second"] = dim["second"].astype(int)
    dim["is_night"] = (dim["hour"] >= 20) | (dim["hour"] < 6)

    dim = dim.reset_index(drop=True)
    dim.insert(0, "time_key", dim.index + 1)
    return dim


def _build_dim_date(segments_df: pd.DataFrame) -> pd.DataFrame:
    if "service_date" in segments_df.columns:
        dates = pd.to_datetime(
            segments_df["service_date"],
            errors="coerce",
            format="%Y%m%d",
        )
    else:
        dates = pd.to_datetime(segments_df["load_timestamp"], errors="coerce")

    dates = dates.dt.date.dropna().drop_duplicates().reset_index(drop=True)
    dim = pd.DataFrame({"date_value": dates})
    dim["date_key"] = dim["date_value"].apply(lambda d: int(d.strftime("%Y%m%d")))
    dim["year"] = dim["date_value"].apply(lambda d: d.year)
    dim["month"] = dim["date_value"].apply(lambda d: d.month)
    dim["day"] = dim["date_value"].apply(lambda d: d.day)
    dim = dim[["date_key", "date_value", "year", "month", "day"]]
    return dim


def _build_fact_segments(
    segments_df: pd.DataFrame,
    dim_country: pd.DataFrame,
    dim_operator: pd.DataFrame,
    dim_station: pd.DataFrame,
    dim_route: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_date: pd.DataFrame,
    country_mapping: dict[str, str],
) -> pd.DataFrame:
    fact = segments_df.copy()
    fact["country_code"] = fact["country"].apply(
        lambda value: _map_country(value, country_mapping)
    )

    fact = fact.merge(
        dim_country[["country_key", "country_code"]],
        on="country_code",
        how="left",
    )
    fact = fact.merge(
        dim_operator[["operator_key", "operator_id"]],
        left_on="operator",
        right_on="operator_id",
        how="left",
    )
    fact = fact.merge(
        dim_route[["route_key", "route_id", "operator_id", "country_code"]],
        left_on=["route_id", "operator", "country_code"],
        right_on=["route_id", "operator_id", "country_code"],
        how="left",
    )
    fact = fact.merge(
        dim_station[["station_key", "stop_id", "country_code"]],
        left_on=["departure_stop_id", "country_code"],
        right_on=["stop_id", "country_code"],
        how="left",
    ).rename(columns={"station_key": "departure_station_key"})
    fact = fact.merge(
        dim_station[["station_key", "stop_id", "country_code"]],
        left_on=["arrival_stop_id", "country_code"],
        right_on=["stop_id", "country_code"],
        how="left",
    ).rename(columns={"station_key": "arrival_station_key"})

    fact["departure_time"] = fact["departure_time"].astype(str)
    fact["arrival_time"] = fact["arrival_time"].astype(str)

    fact = fact.merge(
        dim_time[["time_key", "time_value"]],
        left_on="departure_time",
        right_on="time_value",
        how="left",
    ).rename(columns={"time_key": "departure_time_key"})
    fact = fact.merge(
        dim_time[["time_key", "time_value"]],
        left_on="arrival_time",
        right_on="time_value",
        how="left",
    ).rename(columns={"time_key": "arrival_time_key"})

    if "service_date" in fact.columns:
        fact["date_value"] = pd.to_datetime(
            fact["service_date"],
            errors="coerce",
            format="%Y%m%d",
        ).dt.date
    else:
        fact["date_value"] = pd.to_datetime(fact["load_timestamp"], errors="coerce").dt.date
    fact = fact.merge(
        dim_date[["date_key", "date_value"]],
        on="date_value",
        how="left",
    )

    fact = fact.rename(columns={"trip_id": "trip_business_id"})

    fact = fact[
        [
            "country_key",
            "operator_key",
            "route_key",
            "departure_station_key",
            "arrival_station_key",
            "departure_time_key",
            "arrival_time_key",
            "date_key",
            "trip_business_id",
            "is_night",
            "is_cross_border",
        ]
    ]

    fact = fact.reset_index(drop=True)
    fact.insert(0, "fact_trip_key", fact.index + 1)

    key_cols = [
        "fact_trip_key",
        "country_key",
        "operator_key",
        "route_key",
        "departure_station_key",
        "arrival_station_key",
        "departure_time_key",
        "arrival_time_key",
        "date_key",
    ]
    for col in key_cols:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")

    return fact


def _extract_segments_from_zip(content: bytes, country: str, operator: str) -> pd.DataFrame:
    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            trips_df = _read_zip_csv(zf, "trips.txt", ["trip_id", "route_id", "service_id"])
            stop_times_df = _read_zip_csv(
                zf,
                "stop_times.txt",
                ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
            )
            stops_df = _read_zip_csv(
                zf,
                "stops.txt",
                ["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_country"],
            )
            calendar_df = _read_zip_csv(
                zf,
                "calendar.txt",
                ["service_id", "start_date", "end_date"],
            )
            calendar_dates_df = _read_zip_csv(
                zf,
                "calendar_dates.txt",
                ["service_id", "date", "exception_type"],
            )
    except zipfile.BadZipFile:
        LOGGER.warning("Fichier non ZIP pour %s (%s).", operator, country)
        return pd.DataFrame()

    if trips_df.empty or stop_times_df.empty or stops_df.empty:
        return pd.DataFrame()

    stop_times_df = stop_times_df.sort_values(["trip_id", "stop_sequence"])
    first_stop = stop_times_df.groupby("trip_id").first().reset_index()
    last_stop = stop_times_df.groupby("trip_id").last().reset_index()

    first_stop = first_stop.rename(
        columns={
            "stop_id": "departure_stop_id",
            "arrival_time": "departure_time",
            "departure_time": "departure_time_raw",
        }
    )
    first_stop["departure_time"] = first_stop["departure_time"].fillna(
        first_stop["departure_time_raw"]
    )

    last_stop = last_stop.rename(
        columns={
            "stop_id": "arrival_stop_id",
            "arrival_time": "arrival_time",
            "departure_time": "arrival_time_raw",
        }
    )
    last_stop["arrival_time"] = last_stop["arrival_time"].fillna(
        last_stop["arrival_time_raw"]
    )

    merged = trips_df.merge(first_stop, on="trip_id").merge(last_stop, on="trip_id")

    service_date_map = _build_service_date_map(calendar_df, calendar_dates_df)
    merged["service_date"] = merged["service_id"].map(service_date_map)

    stop_cols = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    if "stop_country" in stops_df.columns:
        stop_cols.append("stop_country")

    merged = merged.merge(
        stops_df[stop_cols],
        left_on="departure_stop_id",
        right_on="stop_id",
        how="left",
        suffixes=("", "_departure"),
    )
    merged = merged.merge(
        stops_df[stop_cols],
        left_on="arrival_stop_id",
        right_on="stop_id",
        how="left",
        suffixes=("_departure", "_arrival"),
    )

    if "stop_country" in stops_df.columns:
        merged["is_cross_border"] = merged.apply(
            lambda row: _is_cross_border_by_stops(
                stops_df,
                row.get("departure_stop_id"),
                row.get("arrival_stop_id"),
            ),
            axis=1,
        )
    else:
        merged["is_cross_border"] = False

    for _, row in merged.iterrows():
        rows.append(
            {
                "country": _normalize_country(country),
                "operator": operator,
                "trip_id": row.get("trip_id"),
                "route_id": row.get("route_id"),
                "departure_stop_id": row.get("departure_stop_id"),
                "arrival_stop_id": row.get("arrival_stop_id"),
                "departure_time": _normalize_time(row.get("departure_time")),
                "arrival_time": _normalize_time(row.get("arrival_time")),
                "departure_station": _normalize_station_name(row.get("stop_name_departure")),
                "arrival_station": _normalize_station_name(row.get("stop_name_arrival")),
                "departure_lat": row.get("stop_lat_departure"),
                "departure_lon": row.get("stop_lon_departure"),
                "arrival_lat": row.get("stop_lat_arrival"),
                "arrival_lon": row.get("stop_lon_arrival"),
                "is_cross_border": row.get("is_cross_border"),
                "service_date": row.get("service_date"),
            }
        )

    return pd.DataFrame(rows)


def _extract_trip_stops_from_zip(content: bytes, country: str, operator: str) -> pd.DataFrame:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            trips_df = _read_zip_csv(zf, "trips.txt", ["trip_id", "service_id"])
            stop_times_df = _read_zip_csv(
                zf,
                "stop_times.txt",
                ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
            )
            stops_df = _read_zip_csv(
                zf,
                "stops.txt",
                ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            )
            calendar_df = _read_zip_csv(
                zf,
                "calendar.txt",
                ["service_id", "start_date", "end_date"],
            )
            calendar_dates_df = _read_zip_csv(
                zf,
                "calendar_dates.txt",
                ["service_id", "date", "exception_type"],
            )
    except zipfile.BadZipFile:
        return pd.DataFrame()

    if trips_df.empty or stop_times_df.empty or stops_df.empty:
        return pd.DataFrame()

    service_date_map = _build_service_date_map(calendar_df, calendar_dates_df)

    stop_times_df["stop_sequence"] = pd.to_numeric(
        stop_times_df["stop_sequence"], errors="coerce"
    )

    merged = stop_times_df.merge(trips_df, on="trip_id", how="left")
    merged = merged.merge(stops_df, on="stop_id", how="left")

    merged["service_date"] = merged["service_id"].map(service_date_map)

    merged["arrival_time"] = merged["arrival_time"].apply(_normalize_time)
    merged["departure_time"] = merged["departure_time"].apply(_normalize_time)

    merged["country_code"] = _normalize_country(country)
    merged["operator_id"] = operator

    merged = merged.rename(
        columns={
            "stop_name": "stop_name",
            "stop_lat": "stop_lat",
            "stop_lon": "stop_lon",
        }
    )

    merged = merged[
        [
            "country_code",
            "operator_id",
            "trip_id",
            "stop_sequence",
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "arrival_time",
            "departure_time",
            "service_date",
        ]
    ]

    return merged


def _load_geo() -> pd.DataFrame:
    data = _download_bytes(GEO_URL)
    return pd.read_csv(io.BytesIO(data), low_memory=False)


def _load_night_trains() -> pd.DataFrame:
    data = _download_bytes(NIGHT_TRAINS_URL)
    df = pd.read_csv(io.BytesIO(data), low_memory=False)
    return df


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "obrail"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )


def _copy_df(cur, table_name: str, df: pd.DataFrame) -> None:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", buffer)


def run_stream_etl(country_codes: set[str] | None = None) -> None:
    _setup_logger()

    target_codes = country_codes or _parse_country_codes(os.environ.get("TARGET_COUNTRIES"))
    if not target_codes:
        target_codes = DEFAULT_TARGET_COUNTRIES
    max_per_country = int(os.environ.get("MAX_FEEDS_PER_COUNTRY", "3"))

    sources = [
        src for src in GTFS_SOURCES if _match_country(target_codes, src.get("country"))
    ]

    priority_sources = get_mobility_database_sources(
        country_codes={c.upper() for c in target_codes},
        max_per_country=max_per_country,
    )
    sources.extend(priority_sources)

    LOGGER.info("Sources GTFS en streaming: %s", sources)

    segments_list = []
    trip_stops_list = []
    for src in sources:
        url = src.get("url")
        country = src.get("country")
        operator = src.get("operator")
        if not url:
            continue
        LOGGER.info("Téléchargement GTFS: %s", url)
        content = _download_bytes(url)
        if not content:
            continue
        df = _extract_segments_from_zip(content, country=country, operator=operator)
        if not df.empty:
            segments_list.append(df)
        stops_df = _extract_trip_stops_from_zip(content, country=country, operator=operator)
        if not stops_df.empty:
            trip_stops_list.append(stops_df)

    if not segments_list:
        LOGGER.warning("Aucun segment GTFS extrait.")
        return

    segments_df = pd.concat(segments_list, ignore_index=True)
    segments_df = transform_trip_segments(segments_df)

    trip_stops_df = pd.DataFrame()
    if trip_stops_list:
        trip_stops_df = pd.concat(trip_stops_list, ignore_index=True)
        trip_stops_df["date_value"] = pd.to_datetime(
            trip_stops_df["service_date"],
            errors="coerce",
            format="%Y%m%d",
        ).dt.date
        trip_stops_df = trip_stops_df.drop(columns=["service_date"], errors="ignore")
        trip_stops_df = trip_stops_df.dropna(subset=["trip_id", "stop_id"]).copy()
        trip_stops_df = trip_stops_df.reset_index(drop=True)
        trip_stops_df.insert(0, "trip_stop_key", trip_stops_df.index + 1)

    night_df = transform_night_trains(_load_night_trains())
    geo_df = _load_geo()

    dim_country = _build_dim_country(geo_df)
    country_mapping = _build_country_mapping(dim_country)
    dim_operator = _build_dim_operator(segments_df, night_df, country_mapping)
    dim_station = _build_dim_station(segments_df, country_mapping)
    dim_route = _build_dim_route(segments_df, country_mapping)
    dim_time = _build_dim_time(segments_df)
    dim_date = _build_dim_date(segments_df)

    fact_segments = _build_fact_segments(
        segments_df,
        dim_country,
        dim_operator,
        dim_station,
        dim_route,
        dim_time,
        dim_date,
        country_mapping,
    )

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    schema_path = os.path.join(project_root, "data", "scripts", "mart", "schema.sql")

    with _get_conn() as conn:
        with conn.cursor() as cur:
            with open(schema_path, "r", encoding="utf-8") as schema_file:
                cur.execute(schema_file.read())

            cur.execute(
                "TRUNCATE obrail.fact_trip_segment, obrail.dim_time, obrail.dim_date, "
                "obrail.dim_route, obrail.dim_station, obrail.dim_operator, obrail.dim_country, "
                "obrail.trip_stop"
            )

            _copy_df(cur, "obrail.dim_country", dim_country)
            _copy_df(cur, "obrail.dim_operator", dim_operator)
            _copy_df(cur, "obrail.dim_station", dim_station)
            _copy_df(cur, "obrail.dim_route", dim_route)
            _copy_df(cur, "obrail.dim_time", dim_time)
            _copy_df(cur, "obrail.dim_date", dim_date)
            _copy_df(cur, "obrail.fact_trip_segment", fact_segments)
            if not trip_stops_df.empty:
                _copy_df(cur, "obrail.trip_stop", trip_stops_df)

        conn.commit()

    LOGGER.info("ETL streaming terminé à %s", datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    run_stream_etl()
