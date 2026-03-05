"""
Spark-based streaming ETL (no local storage).

Flow:
- Download GTFS ZIPs and CSVs into a temp directory
- Transform with Spark DataFrames
- Build the data mart
- Load directly into PostgreSQL
"""

from __future__ import annotations

import gc
import hashlib
import io
import logging
import os
import shutil
import tempfile
import time
import zipfile
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import urlparse

import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

LOGGER = logging.getLogger("stream_etl_spark")

#MOBILITY_DATABASE_CATALOG_URL = "https://files.mobilitydatabase.org/feeds_v2.csv"
MOBILITY_DATABASE_CATALOG_URL = "C:\\2_EPSI\\MSPR\\mspr_code\\feed_v2_eu27_gtfs_active.csv"
MOBILITY_DATABASE_CATALOG_PATH_ENV = "MOBILITY_DATABASE_CATALOG_PATH"
USE_MOBILITY_DATABASE_SOURCES_ENV = "USE_MOBILITY_DATABASE_SOURCES"
USE_STATIC_GTFS_SOURCES_ENV = "USE_STATIC_GTFS_SOURCES"
GTFS_SOURCES_FILTER_BY_COUNTRY_ENV = "GTFS_SOURCES_FILTER_BY_COUNTRY"
MOBILITY_DATABASE_FEED_IDS_ENV = "MOBILITY_DATABASE_FEED_IDS"
GTFS_LOCAL_DIR_ENV = "GTFS_LOCAL_DIR"
GTFS_LOCAL_COUNTRY_ENV = "GTFS_LOCAL_COUNTRY"
USE_URLS_LATEST_ONLY_ENV = "USE_URLS_LATEST_ONLY"
GTFS_SINGLE_URL_ENV = "GTFS_SINGLE_URL"
GTFS_SINGLE_OPERATOR_ENV = "GTFS_SINGLE_OPERATOR"
GTFS_SINGLE_COUNTRY_ENV = "GTFS_SINGLE_COUNTRY"
NIGHT_TRAINS_PATH_ENV = "NIGHT_TRAINS_PATH"
GEO_PATH_ENV = "GEO_PATH"
SKIP_NIGHT_TRAINS_ENV = "SKIP_NIGHT_TRAINS"
SKIP_GEO_ENV = "SKIP_GEO"
SKIP_TRIP_STOPS_ENV = "SKIP_TRIP_STOPS"
SKIP_STATION_NORMALIZATION_ENV = "SKIP_STATION_NORMALIZATION"
STAGING_MODE_ENV = "STAGING_MODE"
STAGING_DIR_ENV = "STAGING_DIR"
STAGING_COALESCE_ENV = "STAGING_COALESCE"
STAGING_JDBC_TABLE_ENV = "STAGING_JDBC_TABLE"
STAGING_JDBC_TRUNCATE_ENV = "STAGING_JDBC_TRUNCATE"
STAGING_JDBC_BATCHSIZE_ENV = "STAGING_JDBC_BATCHSIZE"
STAGING_JDBC_PARTITIONS_ENV = "STAGING_JDBC_PARTITIONS"
STAGING_JDBC_WRITE_PARTITIONS_ENV = "STAGING_JDBC_WRITE_PARTITIONS"
LOW_MEMORY_MODE_ENV = "LOW_MEMORY_MODE"
BATCH_BY_COUNTRY_ENV = "BATCH_BY_COUNTRY"
CHECKPOINT_MODE_ENV = "CHECKPOINT_MODE"
CHECKPOINT_DIR_ENV = "CHECKPOINT_DIR"
CHECKPOINT_COALESCE_ENV = "CHECKPOINT_COALESCE"
CHECKPOINT_CLEANUP_ENV = "CHECKPOINT_CLEANUP"
SKIP_FEEDS_HTTP_403_ENV = "SKIP_FEEDS_HTTP_403"
SKIP_FEEDS_TIMEOUT_ENV = "SKIP_FEEDS_TIMEOUT"
DISABLE_HADOOP_NATIVE_ENV = "DISABLE_HADOOP_NATIVE"
LOG_FEED_STATS_ENV = "LOG_FEED_STATS"
ALLOWED_ROUTE_TYPES_ENV = "ALLOWED_ROUTE_TYPES"
ALLOW_UNKNOWN_ROUTE_TYPES_ENV = "ALLOW_UNKNOWN_ROUTE_TYPES"
PROFILE_TIMING_ENV = "PROFILE_TIMING"
STATION_NORMALIZATION_COUNTRIES_ENV = "STATION_NORMALIZATION_COUNTRIES"
GTFS_CACHE_DIR_ENV = "GTFS_CACHE_DIR"
GTFS_DOWNLOAD_WORKERS_ENV = "GTFS_DOWNLOAD_WORKERS"
GTFS_DOWNLOAD_TIMEOUT_ENV = "GTFS_DOWNLOAD_TIMEOUT"
GTFS_FEED_PROCESS_WORKERS_ENV = "GTFS_FEED_PROCESS_WORKERS"
GTFS_SEGMENTS_CACHE_DIR_ENV = "GTFS_SEGMENTS_CACHE_DIR"
GTFS_SEGMENTS_CACHE_TTL_HOURS_ENV = "GTFS_SEGMENTS_CACHE_TTL_HOURS"
REQUIRE_TIME_COLUMNS_ENV = "REQUIRE_TIME_COLUMNS"
SKIP_SERVICE_DATES_ENV = "SKIP_SERVICE_DATES"
NIGHT_TRAIN_ROUTE_TYPES_ENV = "NIGHT_TRAIN_ROUTE_TYPES"
NIGHT_TRAIN_USE_TIME_ENV = "NIGHT_TRAIN_USE_TIME"

GTFS_SOURCES = [
    {
        "source": "SNCF Open Data (Horaires SNCF, GTFS)",
        "country": "france",
        "operator": "sncf_voyageurs",
        "url": "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip",
    },
    {
        "source": "OpenMobility",
        "country": "great-britain",
        "operator": "mdb-1311",
        "url": "https://openmobilitydata-data.s3.us-west-1.amazonaws.com/public/feeds/association-of-train-operating-companies/284/20210423/gtfs.zip",
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

CATALOG_COLUMNS = [
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
    "GB": "great-britain",
}

DEFAULT_TARGET_COUNTRIES = {
    "FR",
    #"DE",
    #"GB",
    #"IT",
    #"ES",
    #"NL",
    #"FI",
    #"BE",
    #"CH",
    #"AT",
    #"PL",
    #"SE",
    #"NO",
    #"DK",
}

COORD_ROUND_DECIMALS = 4
MIN_TRIP_DISTANCE_KM = 0
MIN_TRIP_DISTANCE_KM_ENV = "MIN_TRIP_DISTANCE_KM"
DISTANCE_FACTOR = 1.3
COORD_NEAR_ZERO_THRESHOLD = 10
EARTH_RADIUS_KM = 6371.0
DEFAULT_ALLOWED_ROUTE_TYPES = ""#"2,100-117,1100"

STAGING_SEGMENTS_SCHEMA = [
    ("country", "text"),
    ("operator", "text"),
    ("trip_id", "text"),
    ("route_id", "text"),
    ("route_type", "text"),
    ("departure_stop_id", "text"),
    ("arrival_stop_id", "text"),
    ("departure_time", "text"),
    ("arrival_time", "text"),
    ("departure_station", "text"),
    ("arrival_station", "text"),
    ("departure_lat", "text"),
    ("departure_lon", "text"),
    ("arrival_lat", "text"),
    ("arrival_lon", "text"),
    ("is_cross_border", "boolean"),
    ("service_date", "text"),
    ("_stage_id", "bigint"),
]


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


def _timing_enabled() -> bool:
    return os.environ.get(PROFILE_TIMING_ENV, "0").lower() in ("1", "true", "yes")


@contextmanager
def _timed(label: str) -> Iterable[None]:
    if not _timing_enabled():
        yield
        return
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        LOGGER.info("TIMING %s: %.1f ms", label, elapsed_ms)


def _station_normalization_countries() -> set[str]:
    raw = _parse_country_codes(os.environ.get(STATION_NORMALIZATION_COUNTRIES_ENV))
    if not raw:
        return set()
    expanded: set[str] = set()
    for code in raw:
        expanded.add(code)
        alias = COUNTRY_ALIASES.get(code)
        if alias:
            expanded.add(alias.upper())
    return expanded


def _require_time_columns() -> bool:
    return os.environ.get(REQUIRE_TIME_COLUMNS_ENV, "1").lower() in ("1", "true", "yes")


def _night_route_types() -> set[int]:
    return _parse_int_set(os.environ.get(NIGHT_TRAIN_ROUTE_TYPES_ENV))


def _night_use_time() -> bool:
    return os.environ.get(NIGHT_TRAIN_USE_TIME_ENV, "1").lower() in ("1", "true", "yes")


def _skip_service_dates() -> bool:
    return os.environ.get(SKIP_SERVICE_DATES_ENV, "0").lower() in ("1", "true", "yes")


def _critical_columns() -> list[str]:
    cols = ["departure_stop_id", "arrival_stop_id"]
    if _require_time_columns():
        cols.extend(["departure_time", "arrival_time"])
    return cols


def _parse_feed_ids(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip().lower() for item in value.split(",") if item.strip()}


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
    if not url:
        return None
    if os.path.isfile(url):
        try:
            with open(url, "rb") as handle:
                return handle.read()
        except OSError as exc:
            LOGGER.warning("Lecture fichier impossible: %s (%s)", url, exc)
            return None
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        LOGGER.warning("Telechargement impossible: %s (%s)", url, exc)
        return None


def _download_gtfs_bytes(
    url: str,
    fallback_url: str | None = None,
) -> tuple[bytes | None, str | None]:
    def _try_download(target: str) -> tuple[bytes | None, str | None]:
        if not target:
            return None, "empty_url"
        if os.path.isfile(target):
            try:
                with open(target, "rb") as handle:
                    return handle.read(), None
            except OSError as exc:
                LOGGER.warning("Lecture fichier impossible: %s (%s)", target, exc)
                return None, "file_error"
        try:
            timeout = _download_timeout_seconds()
            response = requests.get(target, timeout=timeout)
            response.raise_for_status()
            return response.content, None
        except requests.Timeout:
            return None, "timeout"
        except requests.HTTPError as exc:
            status = None
            if exc.response is not None:
                status = exc.response.status_code
            if status == 403:
                return None, "http_403"
            return None, f"http_{status or 'error'}"
        except requests.RequestException as exc:
            LOGGER.warning("Telechargement impossible: %s (%s)", target, exc)
            return None, "request_error"

    content, err = _try_download(url)
    if content is not None:
        return content, None
    if fallback_url and fallback_url != url:
        fallback_content, fallback_err = _try_download(fallback_url)
        if fallback_content is not None:
            LOGGER.info("Telechargement fallback OK: %s", fallback_url)
            return fallback_content, None
        err = fallback_err or err
    return None, err


def _download_timeout_seconds() -> int:
    raw = os.environ.get(GTFS_DOWNLOAD_TIMEOUT_ENV, "120")
    if raw and raw.isdigit():
        return int(raw)
    return 120


def _cache_path_for_url(url: str, cache_dir: str) -> str:
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1]
    if not ext or len(ext) > 8:
        ext = ".zip"
    key = hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]
    return os.path.join(cache_dir, f"{key}{ext}")


def _download_gtfs_to_cache(
    url: str,
    fallback_url: str | None,
    cache_dir: str,
) -> tuple[str | None, str | None]:
    if not url:
        return None, "empty_url"
    if os.path.isfile(url):
        return url, None

    cache_path = _cache_path_for_url(url, cache_dir)
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        return cache_path, None

    def _try_download(target: str) -> tuple[bytes | None, str | None]:
        if not target:
            return None, "empty_url"
        try:
            timeout = _download_timeout_seconds()
            response = requests.get(target, timeout=timeout)
            response.raise_for_status()
            return response.content, None
        except requests.Timeout:
            return None, "timeout"
        except requests.HTTPError as exc:
            status = None
            if exc.response is not None:
                status = exc.response.status_code
            if status == 403:
                return None, "http_403"
            return None, f"http_{status or 'error'}"
        except requests.RequestException as exc:
            LOGGER.warning("Telechargement impossible: %s (%s)", target, exc)
            return None, "request_error"

    content, err = _try_download(url)
    used_url = url
    if content is None and fallback_url and fallback_url != url:
        content, err = _try_download(fallback_url)
        used_url = fallback_url
        if content is not None:
            LOGGER.info("Telechargement fallback OK: %s", used_url)
    if content is None:
        return None, err

    os.makedirs(cache_dir, exist_ok=True)
    tmp_path = f"{cache_path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp_path, "wb") as handle:
            handle.write(content)
        os.replace(tmp_path, cache_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
    return cache_path, None


def _read_cached_bytes(path: str) -> bytes | None:
    if not path:
        return None
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except OSError as exc:
        LOGGER.warning("Lecture cache impossible: %s (%s)", path, exc)
        return None


def _segments_cache_key(url: str, cache_path: str | None) -> str:
    base = url or ""
    if cache_path and os.path.exists(cache_path):
        stat = os.stat(cache_path)
        base = f"{base}|{stat.st_size}|{int(stat.st_mtime)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:20]


def _segments_cache_path(cache_dir: str, cache_key: str) -> str:
    return os.path.join(cache_dir, cache_key)


def _segments_cache_valid(path: str, ttl_hours: int) -> bool:
    if not path or not os.path.exists(path):
        return False
    if ttl_hours <= 0:
        return True
    age_seconds = time.time() - os.path.getmtime(path)
    return age_seconds <= (ttl_hours * 3600)


def _acquire_cache_lock(cache_path: str) -> str | None:
    lock_path = f"{cache_path}.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.close(fd)
        return lock_path
    except FileExistsError:
        return None
    except OSError:
        return None


def _release_cache_lock(lock_path: str | None) -> None:
    if not lock_path:
        return
    try:
        os.remove(lock_path)
    except OSError:
        pass


def _normalize_country(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _normalize_key(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _parse_int_set(value: str | None) -> set[int]:
    if not value:
        return set()
    result: set[int] = set()
    for raw in value.split(","):
        token = raw.strip()
        if not token:
            continue
        if "-" in token:
            parts = token.split("-", 1)
            if len(parts) != 2:
                continue
            start, end = parts[0].strip(), parts[1].strip()
            if start.isdigit() and end.isdigit():
                lo = int(start)
                hi = int(end)
                if lo <= hi:
                    result.update(range(lo, hi + 1))
                else:
                    result.update(range(hi, lo + 1))
            continue
        if token.isdigit():
            result.add(int(token))
    return result


def _allowed_route_types() -> set[int]:
    raw = os.environ.get(ALLOWED_ROUTE_TYPES_ENV, DEFAULT_ALLOWED_ROUTE_TYPES)
    return _parse_int_set(raw)


def _filter_route_types(df: DataFrame) -> DataFrame:
    if "route_type" not in df.columns:
        LOGGER.warning("Filtrage route_type ignore (colonne manquante).")
        return df
    allowed = _allowed_route_types()
    if not allowed:
        LOGGER.warning("Aucun route_type autorise (ALLOWED_ROUTE_TYPES vide). Filtrage ignore.")
        return df
    allow_unknown = os.environ.get(ALLOW_UNKNOWN_ROUTE_TYPES_ENV, "1").lower() in ("1", "true", "yes")
    tmp = df.withColumn("_route_type_int", F.col("route_type").cast(IntegerType()))
    if allow_unknown:
        tmp = tmp.filter(
            F.col("_route_type_int").isNull() | F.col("_route_type_int").isin(list(allowed))
        )
    else:
        tmp = tmp.filter(F.col("_route_type_int").isin(list(allowed)))
    return tmp.drop("_route_type_int")


def _filter_routes_by_type(routes_df: DataFrame, allow_unknown: bool) -> DataFrame:
    if _df_is_empty(routes_df):
        return routes_df
    if "route_type" not in routes_df.columns:
        return routes_df
    allowed = _allowed_route_types()
    if not allowed:
        return routes_df
    tmp = routes_df.withColumn("_route_type_int", F.col("route_type").cast(IntegerType()))
    if allow_unknown:
        tmp = tmp.filter(
            F.col("_route_type_int").isNull() | F.col("_route_type_int").isin(list(allowed))
        )
    else:
        tmp = tmp.filter(F.col("_route_type_int").isin(list(allowed)))
    return tmp.drop("_route_type_int")

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


def _ensure_catalog_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    defaults = {
        "id": "",
        "data_type": "",
        "location.country_code": "",
        "location.municipality": "",
        "provider": "",
        "is_official": "",
        "name": "",
        "note": "",
        "status": "",
        "urls.latest": "",
        "urls.direct_download": "",
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def _read_catalog_csv(path_or_url: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path_or_url, usecols=CATALOG_COLUMNS, low_memory=False)
        return _ensure_catalog_columns(df)
    except ValueError:
        df = pd.read_csv(path_or_url, low_memory=False)
        return _ensure_catalog_columns(df)


def _load_mobility_catalog() -> pd.DataFrame:
    local_path = os.environ.get(MOBILITY_DATABASE_CATALOG_PATH_ENV)
    if local_path:
        try:
            return _read_catalog_csv(local_path)
        except FileNotFoundError:
            LOGGER.warning("Catalogue Mobility Database introuvable: %s", local_path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Lecture catalogue local impossible (%s): %s", local_path, exc)

    return _read_catalog_csv(MOBILITY_DATABASE_CATALOG_URL)


def _pick_operator_id(row: pd.Series) -> str:
    for field in ("id", "provider", "name"):
        value = row.get(field)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return "unknown_operator"


def _load_local_gtfs_sources(local_dir: str | None, country_code: str | None) -> list[dict]:
    if not local_dir:
        return []
    if not os.path.isdir(local_dir):
        LOGGER.warning("Dossier GTFS local introuvable: %s", local_dir)
        return []

    sources = []
    for root, _, files in os.walk(local_dir):
        for filename in sorted(files):
            if not filename.lower().endswith(".zip"):
                continue
            path = os.path.join(root, filename)
            operator_id = os.path.splitext(filename)[0]
            sources.append(
                {
                    "source": "Local GTFS",
                    "country": (country_code or "").strip(),
                    "operator": operator_id,
                    "url": path,
                }
            )

    if not sources:
        LOGGER.warning("Aucun ZIP GTFS trouve dans: %s", local_dir)
    return sources


def _load_single_gtfs_source() -> list[dict]:
    single_url = os.environ.get(GTFS_SINGLE_URL_ENV)
    if not single_url:
        return []

    operator_id = os.environ.get(GTFS_SINGLE_OPERATOR_ENV)
    if not operator_id:
        basename = os.path.basename(single_url)
        operator_id = os.path.splitext(basename)[0] if basename else "single_source"

    country_code = os.environ.get(GTFS_SINGLE_COUNTRY_ENV, "")

    return [
        {
            "source": "Single GTFS",
            "country": country_code.strip(),
            "operator": operator_id,
            "url": single_url,
        }
    ]


def get_mobility_database_sources(
    country_codes: set[str],
    max_per_country: int = 1,
) -> list[dict]:
    if not country_codes:
        return []

    try:
        df = _load_mobility_catalog()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Catalogue Mobility Database indisponible: %s", exc)
        return []

    df = df.copy()
    df["data_type"] = df["data_type"].astype(str).str.lower()
    df["status"] = df["status"].astype(str).str.lower()
    df["location.country_code"] = df["location.country_code"].astype(str).str.upper()

    df = df[df["data_type"] == "gtfs"]
    df = df[df["status"] == "active"]

    feed_ids = _parse_feed_ids(os.environ.get(MOBILITY_DATABASE_FEED_IDS_ENV))
    if feed_ids:
        df["id"] = df["id"].astype(str).str.strip().str.lower()
        df = df[df["id"].isin(feed_ids)]
        if df.empty:
            LOGGER.warning("Aucun feed Mobility Database pour les IDs: %s", sorted(feed_ids))
            return []
    else:
        df = df[df["location.country_code"].isin({c.upper() for c in country_codes})]

    latest = df["urls.latest"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    direct = df["urls.direct_download"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    prefer_latest = os.environ.get(USE_URLS_LATEST_ONLY_ENV, "0").lower() in (
        "1",
        "true",
        "yes",
    )
    if prefer_latest:
        LOGGER.info(
            "USE_URLS_LATEST_ONLY actif: urls.latest prefere, fallback vers urls.direct_download si besoin."
        )
    df["download_url"] = latest.fillna(direct)
    df = df[df["download_url"].notna()]
    df["fallback_url"] = pd.NA
    df.loc[latest.notna() & direct.notna() & (latest != direct), "fallback_url"] = direct
    if df.empty:
        return []

    df["score"] = df.apply(_score_rail_candidate, axis=1)

    sources = []
    if feed_ids:
        for _, row in df.sort_values("id").iterrows():
            operator_id = _pick_operator_id(row)
            fallback_url = row.get("fallback_url")
            fallback_url = str(fallback_url) if fallback_url is not None else ""
            sources.append(
                {
                    "source": "Mobility Database",
                    "country": str(row.get("location.country_code") or "").lower(),
                    "operator": operator_id,
                    "url": str(row.get("download_url")),
                    "fallback_url": fallback_url if fallback_url and fallback_url.lower() != "nan" else "",
                }
            )
        return sources

    for country_code, group in df.groupby("location.country_code"):
        official_group = group[group["is_official"].astype(str).str.lower().isin(["true", "t", "1"])]
        candidate_group = official_group if not official_group.empty else group
        candidate_group = candidate_group.sort_values("score", ascending=False)
        selected = candidate_group.head(max_per_country)
        for _, row in selected.iterrows():
            operator_id = _pick_operator_id(row)
            fallback_url = row.get("fallback_url")
            fallback_url = str(fallback_url) if fallback_url is not None else ""
            sources.append(
                {
                    "source": "Mobility Database",
                    "country": str(country_code).lower(),
                    "operator": operator_id,
                    "url": str(row.get("download_url")),
                    "fallback_url": fallback_url if fallback_url and fallback_url.lower() != "nan" else "",
                }
            )
    return sources

def _build_spark_session() -> SparkSession:
    def _merge_java_opts(existing: str | None, addition: str) -> str:
        if not existing:
            return addition
        existing = existing.strip()
        if not existing:
            return addition
        if addition in existing:
            return existing
        return f"{existing} {addition}"

    def _append_csv(existing: str | None, value: str) -> str:
        if not existing:
            return value
        parts = [part.strip() for part in existing.split(",") if part.strip()]
        if value in parts:
            return existing
        parts.append(value)
        return ",".join(parts)

    def _find_postgres_jar() -> str | None:
        candidates: list[str] = []
        spark_home = os.environ.get("SPARK_HOME")
        if spark_home:
            candidates.append(os.path.join(spark_home, "jars"))
        try:
            import pyspark  # noqa: WPS433

            candidates.append(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
        except Exception:
            pass
        for jars_dir in candidates:
            if not jars_dir or not os.path.isdir(jars_dir):
                continue
            try:
                for name in os.listdir(jars_dir):
                    lower = name.lower()
                    if lower.startswith("postgresql-") and lower.endswith(".jar"):
                        return os.path.join(jars_dir, name)
            except OSError:
                continue
        return None

    builder = SparkSession.builder.appName("obrail_stream_etl")
    python_exec = os.environ.get("PYSPARK_PYTHON") or sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)
    builder = builder.config("spark.pyspark.python", python_exec)
    builder = builder.config("spark.pyspark.driver.python", python_exec)
    low_mem = os.environ.get(LOW_MEMORY_MODE_ENV, "1").lower() in ("1", "true", "yes")
    master = os.environ.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    elif low_mem:
        builder = builder.master("local[2]")
    jars = os.environ.get("SPARK_JARS")
    packages = os.environ.get("SPARK_JARS_PACKAGES")
    has_pg_driver = False
    if jars and "postgresql" in jars.lower():
        has_pg_driver = True
    if packages and "org.postgresql:postgresql" in packages.lower():
        has_pg_driver = True
    pg_jar = os.environ.get("PG_JDBC_JAR")
    if pg_jar:
        if os.path.isfile(pg_jar):
            jars = _append_csv(jars, pg_jar)
            has_pg_driver = True
        else:
            LOGGER.warning("PG_JDBC_JAR introuvable: %s", pg_jar)
    if not has_pg_driver:
        discovered = _find_postgres_jar()
        if discovered:
            jars = _append_csv(jars, discovered)
            has_pg_driver = True
    pg_pkg = os.environ.get("PG_JDBC_PACKAGE")
    if not has_pg_driver and pg_pkg:
        packages = _append_csv(packages, pg_pkg)
        has_pg_driver = True
    if jars:
        builder = builder.config("spark.jars", jars)
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    if not has_pg_driver:
        LOGGER.warning(
            "Driver PostgreSQL JDBC manquant. Definis PG_JDBC_JAR, PG_JDBC_PACKAGE, "
            "ou SPARK_JARS/SPARK_JARS_PACKAGES avant de lancer Spark.",
        )
    local_dir = os.environ.get("SPARK_LOCAL_DIR")
    if not local_dir:
        if low_mem:
            local_dir = "C:\\spark-tmp"
            try:
                os.makedirs(local_dir, exist_ok=True)
            except OSError:
                local_dir = None
        if not local_dir:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            local_dir = os.path.join(project_root, "data", "tmp", "spark_local")
            os.makedirs(local_dir, exist_ok=True)
    builder = builder.config("spark.local.dir", local_dir)
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY")
    if driver_mem:
        builder = builder.config("spark.driver.memory", driver_mem)
    elif low_mem:
        builder = builder.config("spark.driver.memory", "4g")
    executor_mem = os.environ.get("SPARK_EXECUTOR_MEMORY")
    if executor_mem:
        builder = builder.config("spark.executor.memory", executor_mem)
    elif low_mem:
        builder = builder.config("spark.executor.memory", "4g")
    shuffle_parts = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS")
    if shuffle_parts:
        builder = builder.config("spark.sql.shuffle.partitions", shuffle_parts)
    elif low_mem:
        builder = builder.config("spark.sql.shuffle.partitions", "64")
    parallelism = os.environ.get("SPARK_DEFAULT_PARALLELISM")
    if parallelism:
        builder = builder.config("spark.default.parallelism", parallelism)
    elif low_mem:
        builder = builder.config("spark.default.parallelism", "64")
    max_part_bytes = os.environ.get("SPARK_SQL_FILES_MAX_PARTITION_BYTES")
    if max_part_bytes:
        builder = builder.config("spark.sql.files.maxPartitionBytes", max_part_bytes)
    elif low_mem:
        builder = builder.config("spark.sql.files.maxPartitionBytes", "67108864")
    builder = builder.config("spark.python.worker.faulthandler.enabled", "true")
    builder = builder.config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    builder = builder.config("spark.sql.files.ignoreMissingFiles", "true")
    builder = builder.config("spark.sql.files.ignoreCorruptFiles", "true")
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    if low_mem:
        builder = builder.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864")
        builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if os.name == "nt":
        # Windows: reduce native IO usage and avoid commit path listing issues.
        builder = builder.config("spark.hadoop.fs.file.impl.disable.cache", "true")
        builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        disable_native_env = os.environ.get(DISABLE_HADOOP_NATIVE_ENV)
        hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("HADOOP_HOME_DIR")
        if not hadoop_home:
            for candidate in ("C:\\hadoop", "C:\\winutils"):
                if os.path.exists(os.path.join(candidate, "bin", "winutils.exe")):
                    hadoop_home = candidate
                    os.environ.setdefault("HADOOP_HOME", candidate)
                    break
        bin_dir = None
        has_hadoop_dll = False
        if hadoop_home:
            builder = builder.config("spark.hadoop.hadoop.home.dir", hadoop_home)
            bin_dir = os.path.join(hadoop_home, "bin")
            if os.path.isdir(bin_dir):
                has_hadoop_dll = os.path.exists(os.path.join(bin_dir, "hadoop.dll"))

        if disable_native_env is not None:
            disable_native = disable_native_env.lower() in ("1", "true", "yes")
        else:
            # Default to disabling native Hadoop IO on Windows unless explicitly enabled.
            disable_native = True

        if disable_native:
            LOGGER.info(
                "Windows: Hadoop native IO disabled (set %s=0 to enable if winutils/hadoop.dll are installed).",
                DISABLE_HADOOP_NATIVE_ENV,
            )
            builder = builder.config("spark.hadoop.io.native.lib.available", "false")
            builder = builder.config("spark.hadoop.hadoop.native.lib", "false")
            extra = "-Dhadoop.native.lib=false -Dio.native.lib.available=false -Djava.library.path="
            driver_opts = _merge_java_opts(
                os.environ.get("SPARK_DRIVER_EXTRA_JAVA_OPTIONS"),
                extra,
            )
            executor_opts = _merge_java_opts(
                os.environ.get("SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS"),
                extra,
            )
            builder = builder.config("spark.driver.extraJavaOptions", driver_opts)
            builder = builder.config("spark.executor.extraJavaOptions", executor_opts)
            if bin_dir and os.environ.get("PATH"):
                path_entries = os.environ["PATH"].split(os.pathsep)
                filtered = [
                    entry
                    for entry in path_entries
                    if os.path.normcase(entry) != os.path.normcase(bin_dir)
                ]
                os.environ["PATH"] = os.pathsep.join(filtered)
        else:
            builder = builder.config("spark.hadoop.io.native.lib.available", "true")
            builder = builder.config("spark.hadoop.hadoop.native.lib", "true")
        if not disable_native and bin_dir:
            extra = f"-Djava.library.path={bin_dir}"
            driver_opts = _merge_java_opts(
                os.environ.get("SPARK_DRIVER_EXTRA_JAVA_OPTIONS"),
                extra,
            )
            executor_opts = _merge_java_opts(
                os.environ.get("SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS"),
                extra,
            )
            builder = builder.config("spark.driver.extraJavaOptions", driver_opts)
            builder = builder.config("spark.executor.extraJavaOptions", executor_opts)
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    return builder.getOrCreate()


def _spark_empty_df(spark: SparkSession, columns: Iterable[str]) -> DataFrame:
    cols = list(columns)
    base = spark.range(0)
    if not cols:
        return base.select(F.lit(1).alias("_dummy")).limit(0).drop("_dummy")
    return base.select([F.lit(None).cast(StringType()).alias(col) for col in cols])


def _spark_empty_df_schema(spark: SparkSession, schema: StructType) -> DataFrame:
    base = spark.range(0)
    if not schema or not schema.fields:
        return base.select(F.lit(1).alias("_dummy")).limit(0).drop("_dummy")
    return base.select(
        [F.lit(None).cast(field.dataType).alias(field.name) for field in schema.fields]
    )


def _df_is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def _is_within_dir(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([os.path.abspath(path), os.path.abspath(root)]) == os.path.abspath(root)
    except ValueError:
        return False


def _read_gtfs_csv(
    spark: SparkSession,
    base_dir: str,
    filename: str,
    columns: list[str],
) -> DataFrame:
    path = os.path.join(base_dir, filename)
    if not os.path.exists(path):
        return _spark_empty_df(spark, columns)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    for col in columns:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(StringType()))
    return df.select([F.col(col) for col in columns])


def _resolve_parent_stops(stops_df: DataFrame) -> DataFrame:
    if _df_is_empty(stops_df):
        return stops_df

    if "parent_station" not in stops_df.columns:
        stops_df = stops_df.withColumn("parent_station", F.lit(""))
    if "location_type" not in stops_df.columns:
        stops_df = stops_df.withColumn("location_type", F.lit(""))

    stops_df = stops_df.withColumn("parent_station", F.trim(F.col("parent_station")))
    parent_lookup = stops_df.select(
        F.col("stop_id").alias("parent_stop_id"),
        F.col("stop_name").alias("parent_stop_name"),
        F.col("stop_lat").alias("parent_stop_lat"),
        F.col("stop_lon").alias("parent_stop_lon"),
    )

    joined = stops_df.join(
        parent_lookup,
        stops_df["parent_station"] == parent_lookup["parent_stop_id"],
        "left",
    )

    has_parent = (
        F.col("parent_station").isNotNull()
        & (F.trim(F.col("parent_station")) != "")
        & (F.lower(F.col("parent_station")) != "nan")
    )

    joined = joined.withColumn(
        "stop_name",
        F.when(has_parent & F.col("parent_stop_name").isNotNull(), F.col("parent_stop_name")).otherwise(
            F.col("stop_name")
        ),
    )
    joined = joined.withColumn(
        "stop_lat",
        F.when(has_parent & F.col("parent_stop_lat").isNotNull(), F.col("parent_stop_lat")).otherwise(
            F.col("stop_lat")
        ),
    )
    joined = joined.withColumn(
        "stop_lon",
        F.when(has_parent & F.col("parent_stop_lon").isNotNull(), F.col("parent_stop_lon")).otherwise(
            F.col("stop_lon")
        ),
    )
    return joined.drop("parent_stop_id", "parent_stop_name", "parent_stop_lat", "parent_stop_lon")


def _filter_coord_pairs(df: DataFrame, lat_col: str, lon_col: str, label: str) -> DataFrame:
    if lat_col not in df.columns or lon_col not in df.columns:
        LOGGER.warning("%s coords ignorees (colonnes manquantes).", label)
        return df

    df = df.withColumn(lat_col, F.col(lat_col).cast(DoubleType()))
    df = df.withColumn(lon_col, F.col(lon_col).cast(DoubleType()))
    cond = (
        F.col(lat_col).between(-90, 90)
        & F.col(lon_col).between(-180, 180)
        & (F.col(lat_col) != 0)
        & (F.col(lon_col) != 0)
        & ~(
            (F.abs(F.col(lat_col)) < COORD_NEAR_ZERO_THRESHOLD)
            & (F.abs(F.col(lon_col)) < COORD_NEAR_ZERO_THRESHOLD)
        )
    )
    return df.filter(cond)


def _filter_valid_coords(df: DataFrame) -> DataFrame:
    coord_cols = [
        "departure_lat",
        "departure_lon",
        "arrival_lat",
        "arrival_lon",
    ]
    missing = [col for col in coord_cols if col not in df.columns]
    if missing:
        LOGGER.warning("Filtrage coords ignore (colonnes manquantes): %s", missing)
        return df

    for col in coord_cols:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))
    cond = (
        F.col("departure_lat").between(-90, 90)
        & F.col("arrival_lat").between(-90, 90)
        & F.col("departure_lon").between(-180, 180)
        & F.col("arrival_lon").between(-180, 180)
        & (F.col("departure_lat") != 0)
        & (F.col("arrival_lat") != 0)
        & (F.col("departure_lon") != 0)
        & (F.col("arrival_lon") != 0)
    )
    return df.filter(cond)


def _normalize_time_col(col: F.Column) -> F.Column:
    text = F.trim(col.cast(StringType()))
    parts = F.split(text, ":")
    hour = (parts.getItem(0).cast(IntegerType()) % 24)
    minute = parts.getItem(1)
    second = F.coalesce(parts.getItem(2), F.lit("00"))
    hour_str = F.lpad(hour.cast(StringType()), 2, "0")
    minute_str = F.lpad(minute, 2, "0")
    second_str = F.lpad(second, 2, "0")
    formatted = F.concat_ws(":", hour_str, minute_str, second_str)
    return (
        F.when(text.isNull() | (text == ""), F.lit(None))
        .when(F.size(parts) < 2, text)
        .otherwise(formatted)
    )


def _is_night_col(col: F.Column) -> F.Column:
    parts = F.split(F.trim(col.cast(StringType())), ":")
    hour = (parts.getItem(0).cast(IntegerType()) % 24)
    return (
        F.when(col.isNull(), F.lit(False))
        .when(hour >= 20, F.lit(True))
        .when(hour < 6, F.lit(True))
        .otherwise(F.lit(False))
    )

def _normalize_station_names_by_coords(df: DataFrame) -> DataFrame:
    required = [
        "departure_station",
        "arrival_station",
        "departure_lat",
        "departure_lon",
        "arrival_lat",
        "arrival_lon",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        LOGGER.warning("Normalisation stations ignoree (colonnes manquantes): %s", missing)
        return df

    df = df.withColumn("_dep_lat_r", F.round(F.col("departure_lat").cast(DoubleType()), COORD_ROUND_DECIMALS))
    df = df.withColumn("_dep_lon_r", F.round(F.col("departure_lon").cast(DoubleType()), COORD_ROUND_DECIMALS))
    df = df.withColumn("_arr_lat_r", F.round(F.col("arrival_lat").cast(DoubleType()), COORD_ROUND_DECIMALS))
    df = df.withColumn("_arr_lon_r", F.round(F.col("arrival_lon").cast(DoubleType()), COORD_ROUND_DECIMALS))
    df = df.withColumn("__order", F.monotonically_increasing_id())

    spark = df.sparkSession
    schema = StructType(
        [
            StructField("__key", StructType([StructField("lat", DoubleType()), StructField("lon", DoubleType())])),
            StructField("__name_norm", StringType()),
            StructField("__order", LongType()),
            StructField("__priority", IntegerType()),
        ]
    )
    combined = _spark_empty_df_schema(spark, schema)

    def _build_part(station_col: str, lat_col: str, lon_col: str, priority: int) -> DataFrame:
        tmp = df.select(
            F.col(station_col).alias("__name"),
            F.col(lat_col).alias("__lat"),
            F.col(lon_col).alias("__lon"),
            F.col("__order"),
        )
        tmp = tmp.filter(
            F.col("__lat").isNotNull()
            & F.col("__lon").isNotNull()
            & F.col("__name").isNotNull()
            & (F.trim(F.col("__name")) != "")
            & (F.lower(F.col("__name")) != "nan")
        )
        tmp = tmp.withColumn("__key", F.struct(F.col("__lat").alias("lat"), F.col("__lon").alias("lon")))
        tmp = tmp.withColumn("__name_norm", F.trim(F.col("__name")))
        tmp = tmp.withColumn("__priority", F.lit(priority))
        return tmp.select("__key", "__name_norm", "__order", "__priority")

    dep = _build_part("departure_station", "_dep_lat_r", "_dep_lon_r", 0)
    arr = _build_part("arrival_station", "_arr_lat_r", "_arr_lon_r", 1)
    combined = combined.unionByName(dep, allowMissingColumns=True).unionByName(arr, allowMissingColumns=True)

    if _df_is_empty(combined):
        return df.drop("_dep_lat_r", "_dep_lon_r", "_arr_lat_r", "_arr_lon_r", "__order")

    grouped = combined.groupBy("__key", "__name_norm").agg(
        F.count(F.lit(1)).alias("cnt"),
        F.min("__order").alias("first_order"),
    )
    grouped = grouped.withColumn("name_len", F.length(F.col("__name_norm")))
    win = Window.partitionBy("__key").orderBy(
        F.desc("cnt"), F.desc("name_len"), F.asc("first_order")
    )
    mapping = grouped.withColumn("rn", F.row_number().over(win)).filter(F.col("rn") == 1).select(
        "__key",
        "__name_norm",
    )

    df = df.withColumn("_dep_key", F.struct(F.col("_dep_lat_r").alias("lat"), F.col("_dep_lon_r").alias("lon")))
    df = df.join(
        mapping.withColumnRenamed("__name_norm", "dep_name"),
        df["_dep_key"] == mapping["__key"],
        "left",
    ).drop(mapping["__key"])
    df = df.withColumn("departure_station", F.coalesce(F.col("dep_name"), F.col("departure_station")))

    df = df.withColumn("_arr_key", F.struct(F.col("_arr_lat_r").alias("lat"), F.col("_arr_lon_r").alias("lon")))
    df = df.join(
        mapping.withColumnRenamed("__name_norm", "arr_name"),
        df["_arr_key"] == mapping["__key"],
        "left",
    ).drop(mapping["__key"])
    df = df.withColumn("arrival_station", F.coalesce(F.col("arr_name"), F.col("arrival_station")))

    return df.drop(
        "_dep_lat_r",
        "_dep_lon_r",
        "_arr_lat_r",
        "_arr_lon_r",
        "_dep_key",
        "_arr_key",
        "dep_name",
        "arr_name",
        "__order",
    )


def _normalize_station_coords_by_name(df: DataFrame) -> DataFrame:
    required = [
        "departure_station",
        "arrival_station",
        "departure_lat",
        "departure_lon",
        "arrival_lat",
        "arrival_lon",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        LOGGER.warning("Normalisation coords ignoree (colonnes manquantes): %s", missing)
        return df

    df = df.withColumn("__order2", F.monotonically_increasing_id())
    spark = df.sparkSession
    schema = StructType(
        [
            StructField("__name", StringType()),
            StructField("__lat_r", DoubleType()),
            StructField("__lon_r", DoubleType()),
            StructField("__order", LongType()),
            StructField("__priority", IntegerType()),
        ]
    )
    combined = _spark_empty_df_schema(spark, schema)

    def _build_part(station_col: str, lat_col: str, lon_col: str, priority: int) -> DataFrame:
        tmp = df.select(
            F.trim(F.col(station_col)).alias("__name"),
            F.col(lat_col).cast(DoubleType()).alias("__lat"),
            F.col(lon_col).cast(DoubleType()).alias("__lon"),
            F.col("__order2").alias("__order"),
        )
        tmp = tmp.filter(
            F.col("__name").isNotNull()
            & (F.trim(F.col("__name")) != "")
            & (F.lower(F.col("__name")) != "nan")
            & F.col("__lat").isNotNull()
            & F.col("__lon").isNotNull()
        )
        tmp = tmp.withColumn("__lat_r", F.round(F.col("__lat"), COORD_ROUND_DECIMALS))
        tmp = tmp.withColumn("__lon_r", F.round(F.col("__lon"), COORD_ROUND_DECIMALS))
        tmp = tmp.withColumn("__priority", F.lit(priority))
        return tmp.select("__name", "__lat_r", "__lon_r", "__order", "__priority")

    dep = _build_part("departure_station", "departure_lat", "departure_lon", 0)
    arr = _build_part("arrival_station", "arrival_lat", "arrival_lon", 1)
    combined = combined.unionByName(dep, allowMissingColumns=True).unionByName(arr, allowMissingColumns=True)

    if _df_is_empty(combined):
        return df.drop("__order2")

    grouped = combined.groupBy("__name", "__lat_r", "__lon_r").agg(
        F.count(F.lit(1)).alias("cnt"),
        F.min("__order").alias("first_order"),
        F.min("__priority").alias("first_pri"),
    )
    win = Window.partitionBy("__name").orderBy(
        F.desc("cnt"), F.asc("first_pri"), F.asc("first_order")
    )
    canonical = grouped.withColumn("rn", F.row_number().over(win)).filter(F.col("rn") == 1).select(
        "__name",
        "__lat_r",
        "__lon_r",
    )

    dep_join = canonical.withColumnRenamed("__name", "dep_name").withColumnRenamed("__lat_r", "dep_lat").withColumnRenamed(
        "__lon_r", "dep_lon"
    )
    arr_join = canonical.withColumnRenamed("__name", "arr_name").withColumnRenamed("__lat_r", "arr_lat").withColumnRenamed(
        "__lon_r", "arr_lon"
    )

    df = df.join(dep_join, F.trim(df["departure_station"]) == dep_join["dep_name"], "left")
    df = df.withColumn("departure_lat", F.coalesce(F.col("dep_lat"), F.col("departure_lat")))
    df = df.withColumn("departure_lon", F.coalesce(F.col("dep_lon"), F.col("departure_lon")))
    df = df.drop("dep_name", "dep_lat", "dep_lon")

    df = df.join(arr_join, F.trim(df["arrival_station"]) == arr_join["arr_name"], "left")
    df = df.withColumn("arrival_lat", F.coalesce(F.col("arr_lat"), F.col("arrival_lat")))
    df = df.withColumn("arrival_lon", F.coalesce(F.col("arr_lon"), F.col("arrival_lon")))
    df = df.drop("arr_name", "arr_lat", "arr_lon")

    return df.drop("__order2")


def _haversine_km_expr(lat1: F.Column, lon1: F.Column, lat2: F.Column, lon2: F.Column) -> F.Column:
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return F.lit(EARTH_RADIUS_KM) * c


def _extract_zip_to_temp(content: bytes, root_dir: str) -> str | None:
    if not content:
        return None
    tmp_dir = tempfile.mkdtemp(prefix="gtfs_", dir=root_dir)
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            zf.extractall(tmp_dir)
        return tmp_dir
    except zipfile.BadZipFile:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None


def _build_segments_from_dir(
    spark: SparkSession,
    tmp_dir: str,
    country: str,
    operator: str,
) -> DataFrame:
    trips_df = _read_gtfs_csv(spark, tmp_dir, "trips.txt", ["trip_id", "route_id", "service_id"])
    routes_df = _read_gtfs_csv(spark, tmp_dir, "routes.txt", ["route_id", "route_type"])
    stop_times_df = _read_gtfs_csv(
        spark,
        tmp_dir,
        "stop_times.txt",
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
    )
    stops_df = _read_gtfs_csv(
        spark,
        tmp_dir,
        "stops.txt",
        [
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "stop_country",
            "parent_station",
            "location_type",
        ],
    )
    skip_service_dates = _skip_service_dates()
    calendar_df = None
    calendar_dates_df = None
    if not skip_service_dates:
        calendar_df = _read_gtfs_csv(
            spark,
            tmp_dir,
            "calendar.txt",
            ["service_id", "start_date", "end_date"],
        )
        calendar_dates_df = _read_gtfs_csv(
            spark,
            tmp_dir,
            "calendar_dates.txt",
            ["service_id", "date", "exception_type"],
        )

    if _df_is_empty(trips_df) or _df_is_empty(stop_times_df) or _df_is_empty(stops_df):
        if os.environ.get(LOG_FEED_STATS_ENV, "0").lower() in ("1", "true", "yes"):
            try:
                trips_count = trips_df.count()
                stop_times_count = stop_times_df.count()
                stops_count = stops_df.count()
            except Exception:
                trips_count = stop_times_count = stops_count = -1
            LOGGER.info(
                "Feed %s/%s vide (trips=%s stop_times=%s stops=%s).",
                country,
                operator,
                trips_count,
                stop_times_count,
                stops_count,
            )
        return _spark_empty_df(
            spark,
            [
                "country",
                "operator",
                "trip_id",
                "route_id",
                "route_type",
                "departure_stop_id",
                "arrival_stop_id",
                "departure_time",
                "arrival_time",
                "departure_station",
                "arrival_station",
                "departure_lat",
                "departure_lon",
                "arrival_lat",
                "arrival_lon",
                "is_cross_border",
                "service_date",
            ],
        )

    allow_unknown = os.environ.get(ALLOW_UNKNOWN_ROUTE_TYPES_ENV, "1").lower() in ("1", "true", "yes")
    routes_df = _filter_routes_by_type(routes_df, allow_unknown)
    if not allow_unknown and _df_is_empty(routes_df):
        return _spark_empty_df(
            spark,
            [
                "country",
                "operator",
                "trip_id",
                "route_id",
                "route_type",
                "departure_stop_id",
                "arrival_stop_id",
                "departure_time",
                "arrival_time",
                "departure_station",
                "arrival_station",
                "departure_lat",
                "departure_lon",
                "arrival_lat",
                "arrival_lon",
                "is_cross_border",
                "service_date",
            ],
        )

    if not _df_is_empty(routes_df):
        routes_lookup = routes_df.select("route_id").dropDuplicates()
        trips_df = trips_df.join(routes_lookup, "route_id", "inner")

    stops_df = _resolve_parent_stops(stops_df)
    stops_df = _filter_coord_pairs(stops_df, "stop_lat", "stop_lon", "Stops")
    if _df_is_empty(stops_df):
        if os.environ.get(LOG_FEED_STATS_ENV, "0").lower() in ("1", "true", "yes"):
            LOGGER.info("Feed %s/%s vide apres filtrage coords des stops.", country, operator)
        return _spark_empty_df(
            spark,
            [
                "country",
                "operator",
                "trip_id",
                "route_id",
                "route_type",
                "departure_stop_id",
                "arrival_stop_id",
                "departure_time",
                "arrival_time",
                "departure_station",
                "arrival_station",
                "departure_lat",
                "departure_lon",
                "arrival_lat",
                "arrival_lon",
                "is_cross_border",
                "service_date",
            ],
        )

    stop_times_df = stop_times_df.withColumn("stop_sequence", F.col("stop_sequence").cast(IntegerType()))
    stop_times_df = stop_times_df.filter(F.col("trip_id").isNotNull())
    if not _df_is_empty(trips_df):
        trip_ids = trips_df.select("trip_id").dropDuplicates()
        stop_times_df = stop_times_df.join(trip_ids, "trip_id", "inner")

    seq_bounds = stop_times_df.groupBy("trip_id").agg(
        F.min("stop_sequence").alias("min_seq"),
        F.max("stop_sequence").alias("max_seq"),
    )

    first_candidates = stop_times_df.join(seq_bounds, "trip_id", "inner").filter(
        F.col("stop_sequence") == F.col("min_seq")
    )
    first_stop = first_candidates.groupBy("trip_id").agg(
        F.first("stop_id", ignorenulls=True).alias("departure_stop_id"),
        F.first("arrival_time", ignorenulls=True).alias("departure_time"),
        F.first("departure_time", ignorenulls=True).alias("departure_time_raw"),
    )
    first_stop = first_stop.withColumn(
        "departure_time",
        F.coalesce(F.col("departure_time"), F.col("departure_time_raw")),
    ).drop("departure_time_raw")

    last_candidates = stop_times_df.join(seq_bounds, "trip_id", "inner").filter(
        F.col("stop_sequence") == F.col("max_seq")
    )
    last_stop = last_candidates.groupBy("trip_id").agg(
        F.first("stop_id", ignorenulls=True).alias("arrival_stop_id"),
        F.first("arrival_time", ignorenulls=True).alias("arrival_time"),
        F.first("departure_time", ignorenulls=True).alias("arrival_time_raw"),
    )
    last_stop = last_stop.withColumn(
        "arrival_time",
        F.coalesce(F.col("arrival_time"), F.col("arrival_time_raw")),
    ).drop("arrival_time_raw")

    merged = trips_df.join(first_stop, "trip_id", "inner").join(last_stop, "trip_id", "inner")
    if not _df_is_empty(routes_df):
        merged = merged.join(routes_df, "route_id", "left")
    if "route_type" not in merged.columns:
        merged = merged.withColumn("route_type", F.lit(None).cast(StringType()))

    if skip_service_dates:
        merged = merged.withColumn("service_date", F.lit(None).cast(StringType()))
    else:
        added = calendar_dates_df.filter(F.col("exception_type") == F.lit("1")).groupBy("service_id").agg(
            F.min("date").alias("added_date")
        )
        base = calendar_df.groupBy("service_id").agg(F.min("start_date").alias("start_date"))
        service_dates = base.join(added, "service_id", "left").withColumn(
            "service_date",
            F.coalesce(F.col("added_date"), F.col("start_date")),
        )
        merged = merged.join(service_dates.select("service_id", "service_date"), "service_id", "left")

    stop_cols = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    if "stop_country" in stops_df.columns:
        stop_cols.append("stop_country")

    stops_dep = stops_df.select(
        F.col("stop_id").alias("departure_stop_id"),
        F.col("stop_name").alias("departure_stop_name"),
        F.col("stop_lat").alias("departure_lat"),
        F.col("stop_lon").alias("departure_lon"),
        F.col("stop_country").alias("departure_stop_country") if "stop_country" in stops_df.columns else F.lit(None).alias(
            "departure_stop_country"
        ),
    )
    stops_arr = stops_df.select(
        F.col("stop_id").alias("arrival_stop_id"),
        F.col("stop_name").alias("arrival_stop_name"),
        F.col("stop_lat").alias("arrival_lat"),
        F.col("stop_lon").alias("arrival_lon"),
        F.col("stop_country").alias("arrival_stop_country") if "stop_country" in stops_df.columns else F.lit(None).alias(
            "arrival_stop_country"
        ),
    )

    merged = merged.join(stops_dep, "departure_stop_id", "left").join(stops_arr, "arrival_stop_id", "left")

    if "departure_stop_country" in merged.columns and "arrival_stop_country" in merged.columns:
        merged = merged.withColumn(
            "is_cross_border",
            F.when(
                F.col("departure_stop_country").isNull() | F.col("arrival_stop_country").isNull(),
                F.lit(False),
            ).otherwise(
                F.upper(F.col("departure_stop_country")) != F.upper(F.col("arrival_stop_country"))
            ),
        )
    else:
        merged = merged.withColumn("is_cross_border", F.lit(False))

    segments = merged.select(
        F.lit(_normalize_country(country)).alias("country"),
        F.lit(operator).alias("operator"),
        "trip_id",
        "route_id",
        "route_type",
        "departure_stop_id",
        "arrival_stop_id",
        _normalize_time_col(F.col("departure_time")).alias("departure_time"),
        _normalize_time_col(F.col("arrival_time")).alias("arrival_time"),
        F.initcap(F.trim(F.col("departure_stop_name"))).alias("departure_station"),
        F.initcap(F.trim(F.col("arrival_stop_name"))).alias("arrival_station"),
        "departure_lat",
        "departure_lon",
        "arrival_lat",
        "arrival_lon",
        "is_cross_border",
        "service_date",
    )
    return segments


def _stage_segments_from_zip(
    spark: SparkSession,
    content: bytes,
    country: str,
    operator: str,
    root_dir: str,
    stage_path: str,
    write_mode: str = "append",
) -> None:
    tmp_dir = _extract_zip_to_temp(content, root_dir)
    if not tmp_dir:
        LOGGER.warning("Fichier non ZIP pour %s (%s).", operator, country)
        return
    try:
        segments_df = _build_segments_from_dir(spark, tmp_dir, country=country, operator=operator)
        coalesce_target = os.environ.get(STAGING_COALESCE_ENV)
        if coalesce_target and coalesce_target.isdigit():
            target = int(coalesce_target)
            if target > 0:
                segments_df = segments_df.coalesce(target)
        segments_df.write.mode(write_mode).parquet(stage_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _stage_segments_from_zip_jdbc(
    spark: SparkSession,
    content: bytes,
    country: str,
    operator: str,
    root_dir: str,
    table_name: str,
) -> None:
    tmp_dir = _extract_zip_to_temp(content, root_dir)
    if not tmp_dir:
        LOGGER.warning("Fichier non ZIP pour %s (%s).", operator, country)
        return
    try:
        segments_df = _build_segments_from_dir(spark, tmp_dir, country=country, operator=operator)
        if _df_is_empty(segments_df):
            return
        _stage_segments_to_jdbc(segments_df, table_name)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_segments_from_zip(
    spark: SparkSession,
    content: bytes,
    country: str,
    operator: str,
    root_dir: str,
) -> DataFrame:
    tmp_dir = _extract_zip_to_temp(content, root_dir)
    if not tmp_dir:
        LOGGER.warning("Fichier non ZIP pour %s (%s).", operator, country)
        return _spark_empty_df(
            spark,
            [
                "country",
                "operator",
                "trip_id",
                "route_id",
                "route_type",
                "departure_stop_id",
                "arrival_stop_id",
                "departure_time",
                "arrival_time",
                "departure_station",
                "arrival_station",
                "departure_lat",
                "departure_lon",
                "arrival_lat",
                "arrival_lon",
                "is_cross_border",
                "service_date",
            ],
        )
    return _build_segments_from_dir(spark, tmp_dir, country=country, operator=operator)


def _extract_trip_stops_from_zip(
    spark: SparkSession,
    content: bytes,
    country: str,
    operator: str,
    root_dir: str,
) -> DataFrame:
    def _empty_trip_stops_df() -> DataFrame:
        schema = StructType(
            [
                StructField("country_code", StringType()),
                StructField("operator_id", StringType()),
                StructField("trip_id", StringType()),
                StructField("stop_sequence", IntegerType()),
                StructField("stop_id", StringType()),
                StructField("stop_name", StringType()),
                StructField("stop_lat", StringType()),
                StructField("stop_lon", StringType()),
                StructField("arrival_time", StringType()),
                StructField("departure_time", StringType()),
                StructField("service_date", StringType()),
            ]
        )
        return _spark_empty_df_schema(spark, schema)

    tmp_dir = _extract_zip_to_temp(content, root_dir)
    if not tmp_dir:
        return _empty_trip_stops_df()

    trips_df = _read_gtfs_csv(spark, tmp_dir, "trips.txt", ["trip_id", "service_id"])
    stop_times_df = _read_gtfs_csv(
        spark,
        tmp_dir,
        "stop_times.txt",
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
    )
    stops_df = _read_gtfs_csv(
        spark,
        tmp_dir,
        "stops.txt",
        ["stop_id", "stop_name", "stop_lat", "stop_lon", "parent_station", "location_type"],
    )
    skip_service_dates = _skip_service_dates()
    calendar_df = None
    calendar_dates_df = None
    if not skip_service_dates:
        calendar_df = _read_gtfs_csv(
            spark,
            tmp_dir,
            "calendar.txt",
            ["service_id", "start_date", "end_date"],
        )
        calendar_dates_df = _read_gtfs_csv(
            spark,
            tmp_dir,
            "calendar_dates.txt",
            ["service_id", "date", "exception_type"],
        )

    if _df_is_empty(trips_df) or _df_is_empty(stop_times_df) or _df_is_empty(stops_df):
        return _empty_trip_stops_df()

    stops_df = _resolve_parent_stops(stops_df)
    stop_times_df = stop_times_df.withColumn("stop_sequence", F.col("stop_sequence").cast(IntegerType()))

    merged = stop_times_df.join(trips_df, "trip_id", "left")
    merged = merged.join(stops_df, "stop_id", "left")
    if skip_service_dates:
        merged = merged.withColumn("service_date", F.lit(None).cast(StringType()))
    else:
        added = calendar_dates_df.filter(F.col("exception_type") == F.lit("1")).groupBy("service_id").agg(
            F.min("date").alias("added_date")
        )
        base = calendar_df.groupBy("service_id").agg(F.min("start_date").alias("start_date"))
        service_dates = base.join(added, "service_id", "left").withColumn(
            "service_date",
            F.coalesce(F.col("added_date"), F.col("start_date")),
        )
        merged = merged.join(service_dates.select("service_id", "service_date"), "service_id", "left")

    merged = merged.withColumn("arrival_time", _normalize_time_col(F.col("arrival_time")))
    merged = merged.withColumn("departure_time", _normalize_time_col(F.col("departure_time")))

    merged = merged.withColumn("country_code", F.lit(_normalize_country(country)))
    merged = merged.withColumn("operator_id", F.lit(operator))
    merged = _filter_coord_pairs(merged, "stop_lat", "stop_lon", "Trip stops")

    merged = merged.select(
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
    )
    return merged

def transform_trip_segments(df: DataFrame) -> DataFrame:
    if _df_is_empty(df):
        return df

    with _timed("segments:dedup"):
        dedup_subset = ["trip_id", "departure_stop_id", "arrival_stop_id"]
        if "service_date" in df.columns:
            dedup_subset.append("service_date")
        df = df.dropDuplicates(dedup_subset)

    with _timed("segments:normalize_time"):
        if "service_date" in df.columns:
            df = df.withColumn("service_date", F.trim(F.col("service_date").cast(StringType())))

        df = df.withColumn(
            "departure_time",
            F.when(F.trim(F.col("departure_time")) == "", F.lit(None)).otherwise(F.col("departure_time")),
        )
        df = df.withColumn(
            "arrival_time",
            F.when(F.trim(F.col("arrival_time")) == "", F.lit(None)).otherwise(F.col("arrival_time")),
        )
        df = df.withColumn("departure_time", F.coalesce(F.col("departure_time"), F.col("arrival_time")))

    night_route_types = _night_route_types()
    night_use_time = _night_use_time()
    route_expr = None
    if night_route_types and "route_type" in df.columns:
        route_expr = F.col("route_type").cast(IntegerType()).isin(list(night_route_types))
    time_expr = None
    if night_use_time and "departure_time" in df.columns:
        time_expr = _is_night_col(F.col("departure_time"))
    if route_expr is not None and time_expr is not None:
        df = df.withColumn("is_night", F.when(route_expr, F.lit(True)).otherwise(time_expr))
    elif route_expr is not None:
        df = df.withColumn("is_night", route_expr)
    elif time_expr is not None:
        df = df.withColumn("is_night", time_expr)
    else:
        df = df.withColumn("is_night", F.lit(False))

    if os.environ.get(LOG_FEED_STATS_ENV, "0").lower() in ("1", "true", "yes"):
        with _timed("segments:log_stats"):
            _log_segment_drop_stats(df)

    with _timed("segments:prefilter"):
        df = _filter_route_types(df)
        critical_cols = _critical_columns()
        if critical_cols:
            df = df.dropna(subset=critical_cols)
        df = _filter_valid_coords(df)

        if "departure_stop_id" in df.columns and "arrival_stop_id" in df.columns:
            dep_stop = F.trim(F.col("departure_stop_id"))
            arr_stop = F.trim(F.col("arrival_stop_id"))
            df = df.filter(
                ~(
                    dep_stop.isNotNull()
                    & arr_stop.isNotNull()
                    & (dep_stop != "")
                    & (arr_stop != "")
                    & (dep_stop == arr_stop)
                )
            )

        if "departure_station" in df.columns and "arrival_station" in df.columns:
            dep_name = F.lower(F.trim(F.col("departure_station")))
            arr_name = F.lower(F.trim(F.col("arrival_station")))
            df = df.filter(
                ~(
                    dep_name.isNotNull()
                    & arr_name.isNotNull()
                    & (dep_name != "")
                    & (arr_name != "")
                    & (dep_name == arr_name)
                )
            )

        if all(col in df.columns for col in ["departure_lat", "departure_lon", "arrival_lat", "arrival_lon"]):
            df = df.filter(
                ~(
                    F.col("departure_lat").isNotNull()
                    & F.col("arrival_lat").isNotNull()
                    & F.col("departure_lon").isNotNull()
                    & F.col("arrival_lon").isNotNull()
                    & (F.col("departure_lat") == F.col("arrival_lat"))
                    & (F.col("departure_lon") == F.col("arrival_lon"))
                )
            )

    skip_normalization = os.environ.get(SKIP_STATION_NORMALIZATION_ENV, "0").lower() in (
        "1",
        "true",
        "yes",
    )
    if skip_normalization:
        LOGGER.info("Normalisation stations ignoree (SKIP_STATION_NORMALIZATION=1).")
    else:
        normalization_countries = _station_normalization_countries()
        if normalization_countries and "country" in df.columns:
            LOGGER.info(
                "Normalisation stations limitee aux pays: %s",
                sorted(normalization_countries),
            )
            target = df.filter(
                F.upper(F.col("country")).isin(list(normalization_countries))
            )
            other = df.filter(
                ~F.upper(F.col("country")).isin(list(normalization_countries))
            )
            with _timed("segments:normalize_station"):
                target = _normalize_station_names_by_coords(target)
                target = _normalize_station_coords_by_name(target)
            df = target.unionByName(other, allowMissingColumns=True)
        else:
            if normalization_countries:
                LOGGER.warning(
                    "Normalisation stations par pays ignoree (colonne country manquante)."
                )
            with _timed("segments:normalize_station"):
                df = _normalize_station_names_by_coords(df)
                df = _normalize_station_coords_by_name(df)

    min_trip_km = MIN_TRIP_DISTANCE_KM
    min_trip_env = os.environ.get(MIN_TRIP_DISTANCE_KM_ENV)
    if min_trip_env and min_trip_env.isdigit():
        min_trip_km = int(min_trip_env)

    with _timed("segments:distance"):
        if all(col in df.columns for col in ["departure_lat", "departure_lon", "arrival_lat", "arrival_lon"]):
            dist = _haversine_km_expr(
                F.col("departure_lat"),
                F.col("departure_lon"),
                F.col("arrival_lat"),
                F.col("arrival_lon"),
            )
            dist = F.round(dist * F.lit(DISTANCE_FACTOR), 0)
            df = df.withColumn("distance_km", dist.cast(IntegerType()))
            df = df.filter(F.col("distance_km").isNotNull() & (F.col("distance_km") > 0))
            df = df.filter(F.col("distance_km") >= F.lit(min_trip_km))
        else:
            df = df.withColumn("distance_km", F.lit(None).cast(IntegerType()))
            LOGGER.warning("Calcul distance ignore (colonnes coords manquantes).")

    pair_cols = ["departure_station", "arrival_station"]
    if all(col in df.columns for col in pair_cols):
        dep = F.lower(F.trim(F.col("departure_station")))
        arr = F.lower(F.trim(F.col("arrival_station")))
        df = df.withColumn("_pair_a", F.least(dep, arr))
        df = df.withColumn("_pair_b", F.greatest(dep, arr))
        subset = ["_pair_a", "_pair_b"]
        if "operator" in df.columns:
            subset.insert(0, "operator")
        if "country" in df.columns:
            subset.insert(0, "country")
        df = df.dropDuplicates(subset).drop("_pair_a", "_pair_b")
    else:
        missing = [col for col in pair_cols if col not in df.columns]
        LOGGER.warning("Dedup trajets inverses ignore (colonnes manquantes): %s", missing)

    df = df.withColumn(
        "load_timestamp",
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )
    return df


def _log_segment_drop_stats(df: DataFrame) -> None:
    if _df_is_empty(df):
        return
    if not all(col in df.columns for col in ["country", "operator"]):
        LOGGER.info("Stats feeds ignores (colonnes country/operator manquantes).")
        return

    min_trip_km = MIN_TRIP_DISTANCE_KM
    min_trip_env = os.environ.get(MIN_TRIP_DISTANCE_KM_ENV)
    if min_trip_env and min_trip_env.isdigit():
        min_trip_km = int(min_trip_env)

    base_cols = ["country", "operator"]
    optional_cols = [
        "route_type",
        "departure_stop_id",
        "arrival_stop_id",
        "departure_time",
        "arrival_time",
        "departure_lat",
        "departure_lon",
        "arrival_lat",
        "arrival_lon",
    ]
    cols = base_cols + [col for col in optional_cols if col in df.columns]
    tmp = df.select(*cols)

    critical_cols = _critical_columns()
    missing_required_cols = [col for col in critical_cols if col not in tmp.columns]
    if missing_required_cols:
        LOGGER.info("Stats feeds: colonnes critiques manquantes: %s", missing_required_cols)

    missing_critical = F.lit(False)
    for col in critical_cols:
        if col in tmp.columns:
            missing_critical = missing_critical | F.col(col).isNull()
        else:
            missing_critical = F.lit(True)

    coord_cols = ["departure_lat", "departure_lon", "arrival_lat", "arrival_lon"]
    coords_available = all(col in tmp.columns for col in coord_cols)
    if coords_available:
        dep_lat = F.col("departure_lat").cast(DoubleType())
        dep_lon = F.col("departure_lon").cast(DoubleType())
        arr_lat = F.col("arrival_lat").cast(DoubleType())
        arr_lon = F.col("arrival_lon").cast(DoubleType())
        valid_coords = (
            dep_lat.between(-90, 90)
            & arr_lat.between(-90, 90)
            & dep_lon.between(-180, 180)
            & arr_lon.between(-180, 180)
            & (dep_lat != 0)
            & (arr_lat != 0)
            & (dep_lon != 0)
            & (arr_lon != 0)
        )
        dist = _haversine_km_expr(dep_lat, dep_lon, arr_lat, arr_lon)
        dist = F.round(dist * F.lit(DISTANCE_FACTOR), 0)
        distance_invalid = dist.isNull() | (dist <= 0)
        distance_too_short = dist < F.lit(min_trip_km)
    else:
        valid_coords = F.lit(False)
        distance_invalid = F.lit(True)
        distance_too_short = F.lit(False)

    if "route_type" in df.columns:
        allowed = _allowed_route_types()
        route_type_int = F.col("route_type").cast(IntegerType())
        tmp = tmp.withColumn("_route_type_int", route_type_int)
        tmp = tmp.withColumn("_route_type_missing", F.col("_route_type_int").isNull())
        tmp = tmp.withColumn("_route_type_disallowed", ~F.col("_route_type_int").isin(list(allowed)))
    else:
        tmp = tmp.withColumn("_route_type_missing", F.lit(True))
        tmp = tmp.withColumn("_route_type_disallowed", F.lit(False))

    stats = (
        tmp.withColumn("_missing_critical", missing_critical)
        .withColumn("_valid_coords", valid_coords)
        .withColumn("_distance_invalid", distance_invalid)
        .withColumn("_distance_too_short", distance_too_short)
        .groupBy("country", "operator")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("_route_type_missing"), 1).otherwise(0)).alias("route_type_missing"),
            F.sum(F.when(F.col("_route_type_disallowed"), 1).otherwise(0)).alias("route_type_disallowed"),
            F.sum(F.when(F.col("_missing_critical"), 1).otherwise(0)).alias("missing_critical"),
            F.sum(
                F.when(~F.col("_missing_critical") & ~F.col("_valid_coords"), 1).otherwise(0)
            ).alias("invalid_coords"),
            F.sum(
                F.when(
                    ~F.col("_missing_critical")
                    & F.col("_valid_coords")
                    & F.col("_distance_invalid"),
                    1,
                ).otherwise(0)
            ).alias("distance_invalid"),
            F.sum(
                F.when(
                    ~F.col("_missing_critical")
                    & F.col("_valid_coords")
                    & ~F.col("_distance_invalid")
                    & F.col("_distance_too_short"),
                    1,
                ).otherwise(0)
            ).alias("distance_lt_min"),
        )
    )

    for row in stats.orderBy("country", "operator").collect():
        LOGGER.info(
            "Feed %s/%s total=%s route_type_missing=%s route_type_disallowed=%s missing_critical=%s "
            "invalid_coords=%s distance_invalid=%s distance_lt_min=%s (min_km=%s)",
            row["country"],
            row["operator"],
            row["total"],
            row["route_type_missing"],
            row["route_type_disallowed"],
            row["missing_critical"],
            row["invalid_coords"],
            row["distance_invalid"],
            row["distance_lt_min"],
            min_trip_km,
        )


def transform_night_trains(df: DataFrame) -> DataFrame:
    if _df_is_empty(df):
        return df
    keep_cols = [col for col in ["agency_id", "agency_name", "agency_state", "agency_url"] if col in df.columns]
    df = df.select(*keep_cols)
    df = df.withColumnRenamed("agency_id", "operator_id")
    df = df.withColumnRenamed("agency_name", "operator_name")
    df = df.withColumnRenamed("agency_state", "operator_country")
    df = df.withColumnRenamed("agency_url", "operator_url")
    df = df.withColumn("operator_country", F.upper(F.trim(F.col("operator_country"))))
    df = df.withColumn("is_night", F.lit(True))
    df = df.dropDuplicates()
    df = df.withColumn(
        "load_timestamp",
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )
    return df


def _build_country_mapping(dim_country: DataFrame) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if _df_is_empty(dim_country):
        return mapping
    for row in dim_country.select(
        "country_code",
        "country_name_en",
        "country_name_fr",
        "iso3_code",
    ).collect():
        code = str(row.country_code or "").strip().upper()
        if not code:
            continue
        mapping[_normalize_key(code)] = code
        for field in ("country_name_en", "country_name_fr", "iso3_code"):
            value = getattr(row, field)
            if value:
                mapping[_normalize_key(value)] = code
    return mapping


def _country_map_expr(mapping: dict[str, str]) -> F.Column | None:
    if not mapping:
        return None
    entries: list[F.Column] = []
    for key, value in mapping.items():
        entries.extend([F.lit(key), F.lit(value)])
    return F.create_map(*entries)


def _map_country_col(col: F.Column, mapping_expr: F.Column | None) -> F.Column:
    if mapping_expr is None:
        return F.upper(F.trim(col.cast(StringType())))
    key = F.lower(F.trim(col.cast(StringType())))
    return F.coalesce(mapping_expr[key], F.upper(F.trim(col.cast(StringType()))))


def _build_dim_country(geo_df: DataFrame) -> DataFrame:
    if _df_is_empty(geo_df):
        return geo_df
    geo_df = geo_df.withColumn("CNTR_ID", F.upper(F.col("CNTR_ID")))
    dim = geo_df.select(
        "CNTR_ID",
        "NAME_ENGL",
        "NAME_FREN",
        "ISO3_CODE",
        "EU_STAT",
        "EFTA_STAT",
        "CC_STAT",
    ).dropna(subset=["CNTR_ID"])
    dim = dim.dropDuplicates()
    dim = dim.withColumnRenamed("CNTR_ID", "country_code")
    dim = dim.withColumnRenamed("NAME_ENGL", "country_name_en")
    dim = dim.withColumnRenamed("NAME_FREN", "country_name_fr")
    dim = dim.withColumnRenamed("ISO3_CODE", "iso3_code")
    dim = dim.withColumnRenamed("EU_STAT", "eu_member")
    dim = dim.withColumnRenamed("EFTA_STAT", "efta_member")
    dim = dim.withColumnRenamed("CC_STAT", "candidate_member")
    dim = dim.withColumn("country_name_en", F.trim(F.col("country_name_en")))
    dim = dim.withColumn("country_name_fr", F.trim(F.col("country_name_fr")))
    uk_name = F.lower(F.col("country_name_en")).contains("united kingdom")
    dim = dim.withColumn(
        "country_code",
        F.when(uk_name, F.lit("GB"))
        .when(F.col("country_code") == F.lit("UK"), F.lit("GB"))
        .otherwise(F.col("country_code")),
    )
    dim = dim.withColumn(
        "iso3_code",
        F.when(
            uk_name
            | (
                (F.col("country_code") == F.lit("GB"))
                & (F.col("iso3_code").isNull() | (F.trim(F.col("iso3_code")) == ""))
            ),
            F.lit("GBR"),
        ).otherwise(F.col("iso3_code")),
    )
    dim = dim.groupBy("country_code").agg(
        F.first("country_name_en", ignorenulls=True).alias("country_name_en"),
        F.first("country_name_fr", ignorenulls=True).alias("country_name_fr"),
        F.first("iso3_code", ignorenulls=True).alias("iso3_code"),
        F.first("eu_member", ignorenulls=True).alias("eu_member"),
        F.first("efta_member", ignorenulls=True).alias("efta_member"),
        F.first("candidate_member", ignorenulls=True).alias("candidate_member"),
    )
    dim = dim.withColumn("country_key", F.row_number().over(Window.orderBy("country_code")))
    return dim.select(
        "country_key",
        "country_code",
        "country_name_en",
        "country_name_fr",
        "iso3_code",
        "eu_member",
        "efta_member",
        "candidate_member",
    )


def _build_dim_country_from_segments(segments_df: DataFrame) -> DataFrame:
    schema = StructType(
        [
            StructField("country_key", IntegerType()),
            StructField("country_code", StringType()),
            StructField("country_name_en", StringType()),
            StructField("country_name_fr", StringType()),
            StructField("iso3_code", StringType()),
            StructField("eu_member", StringType()),
            StructField("efta_member", StringType()),
            StructField("candidate_member", StringType()),
        ]
    )
    if _df_is_empty(segments_df) or "country" not in segments_df.columns:
        return _spark_empty_df_schema(segments_df.sparkSession, schema)

    codes = (
        segments_df.select(F.upper(F.trim(F.col("country"))).alias("country_code"))
        .filter(F.col("country_code").isNotNull() & (F.col("country_code") != ""))
        .dropDuplicates()
    )
    if _df_is_empty(codes):
        return _spark_empty_df_schema(segments_df.sparkSession, schema)

    dim = codes.withColumn("country_name_en", F.lit(""))
    dim = dim.withColumn("country_name_fr", F.lit(""))
    dim = dim.withColumn("iso3_code", F.lit(""))
    dim = dim.withColumn("eu_member", F.lit(None).cast(StringType()))
    dim = dim.withColumn("efta_member", F.lit(None).cast(StringType()))
    dim = dim.withColumn("candidate_member", F.lit(None).cast(StringType()))
    dim = dim.withColumn("country_key", F.row_number().over(Window.orderBy("country_code")))
    return dim.select(
        "country_key",
        "country_code",
        "country_name_en",
        "country_name_fr",
        "iso3_code",
        "eu_member",
        "efta_member",
        "candidate_member",
    )


def _build_dim_operator(
    segments_df: DataFrame,
    night_df: DataFrame,
    mapping_expr: F.Column | None,
) -> DataFrame:
    operators_segments = segments_df.select(
        F.col("operator").cast(StringType()).alias("operator_id"),
        F.col("country").alias("operator_country"),
    )
    operators_segments = operators_segments.withColumn(
        "operator_country",
        _map_country_col(F.col("operator_country"), mapping_expr),
    )
    operators_segments = operators_segments.withColumn("operator_name", F.col("operator_id"))
    operators_segments = operators_segments.withColumn("is_night_operator", F.lit(False))

    operators_night = night_df
    if not _df_is_empty(night_df):
        operators_night = night_df.select(
            F.col("operator_id").cast(StringType()).alias("operator_id"),
            F.col("operator_name").cast(StringType()).alias("operator_name"),
            F.col("operator_country").alias("operator_country"),
        )
        operators_night = operators_night.withColumn(
            "operator_country",
            _map_country_col(F.col("operator_country"), mapping_expr),
        )
        operators_night = operators_night.withColumn("is_night_operator", F.lit(True))

    operators = operators_segments.unionByName(operators_night, allowMissingColumns=True)
    operators = operators.filter(F.col("operator_id").isNotNull())

    grouped = operators.groupBy("operator_id").agg(
        F.first("operator_name", ignorenulls=True).alias("operator_name"),
        F.first("operator_country", ignorenulls=True).alias("operator_country"),
        F.max(F.col("is_night_operator").cast(IntegerType())).alias("is_night_operator"),
    )
    grouped = grouped.withColumn("is_night_operator", F.col("is_night_operator") == F.lit(1))
    grouped = grouped.withColumn("operator_key", F.row_number().over(Window.orderBy("operator_id")))
    return grouped.select(
        "operator_key",
        "operator_id",
        "operator_name",
        "operator_country",
        "is_night_operator",
    )


def _build_dim_station(
    segments_df: DataFrame,
    mapping_expr: F.Column | None,
) -> DataFrame:
    dep = segments_df.select(
        F.col("departure_stop_id").alias("stop_id"),
        F.col("departure_station").alias("station_name"),
        F.col("departure_lat").alias("station_lat"),
        F.col("departure_lon").alias("station_lon"),
        F.col("country").alias("country_code"),
    )
    arr = segments_df.select(
        F.col("arrival_stop_id").alias("stop_id"),
        F.col("arrival_station").alias("station_name"),
        F.col("arrival_lat").alias("station_lat"),
        F.col("arrival_lon").alias("station_lon"),
        F.col("country").alias("country_code"),
    )

    stations = dep.unionByName(arr, allowMissingColumns=True)
    stations = stations.filter(F.col("stop_id").isNotNull())

    stations = stations.groupBy("stop_id", "country_code").agg(
        F.first("station_name", ignorenulls=True).alias("station_name"),
        F.first("station_lat", ignorenulls=True).alias("station_lat"),
        F.first("station_lon", ignorenulls=True).alias("station_lon"),
    )

    stations = stations.withColumn("country_code", _map_country_col(F.col("country_code"), mapping_expr))
    stations = stations.withColumn("station_lat", F.col("station_lat").cast(DoubleType()))
    stations = stations.withColumn("station_lon", F.col("station_lon").cast(DoubleType()))
    stations = stations.filter(F.col("station_lat").isNotNull() & F.col("station_lon").isNotNull())
    stations = _filter_coord_pairs(stations, "station_lat", "station_lon", "Stations")
    stations = stations.dropDuplicates(["stop_id", "country_code"])

    stations = stations.withColumn("station_key", F.row_number().over(Window.orderBy("stop_id", "country_code")))
    return stations.select(
        "station_key",
        "stop_id",
        "station_name",
        "station_lat",
        "station_lon",
        "country_code",
    )


def _build_dim_route(
    segments_df: DataFrame,
    mapping_expr: F.Column | None,
) -> DataFrame:
    cols = ["route_id", "operator", "country"]
    if "route_type" in segments_df.columns:
        cols.append("route_type")
    routes = segments_df.select(*cols)
    routes = routes.withColumnRenamed("operator", "operator_id")
    routes = routes.withColumnRenamed("country", "country_code")
    routes = routes.withColumn("country_code", _map_country_col(F.col("country_code"), mapping_expr))

    if "route_type" in routes.columns:
        routes = routes.withColumn("route_type", F.col("route_type").cast(IntegerType()))

    routes = routes.filter(F.col("route_id").isNotNull() & F.col("operator_id").isNotNull()).dropDuplicates()
    routes = routes.withColumn("route_key", F.row_number().over(Window.orderBy("route_id", "operator_id", "country_code")))

    if "route_type" not in routes.columns:
        routes = routes.withColumn("route_type", F.lit(None).cast(IntegerType()))

    return routes.select(
        "route_key",
        "route_id",
        "operator_id",
        "country_code",
        "route_type",
    )


def _build_dim_time(segments_df: DataFrame) -> DataFrame:
    times = segments_df.select(F.col("departure_time").alias("time_value")).unionByName(
        segments_df.select(F.col("arrival_time").alias("time_value"))
    )
    times = times.filter(
        F.col("time_value").isNotNull()
        & (~F.lower(F.trim(F.col("time_value"))).isin(["", "na", "nan", "none"]))
    ).dropDuplicates()

    parts = F.split(F.col("time_value"), ":")
    dim = times.withColumn("hour", parts.getItem(0).cast(IntegerType()))
    dim = dim.withColumn("minute", parts.getItem(1).cast(IntegerType()))
    dim = dim.withColumn("second", F.coalesce(parts.getItem(2), F.lit("0")).cast(IntegerType()))
    dim = dim.filter(F.col("hour").isNotNull() & F.col("minute").isNotNull() & F.col("second").isNotNull())
    dim = dim.withColumn("is_night", (F.col("hour") >= 20) | (F.col("hour") < 6))
    dim = dim.withColumn(
        "time_key",
        (F.col("hour") * F.lit(3600) + F.col("minute") * F.lit(60) + F.col("second")).cast(IntegerType()),
    )
    return dim.select(
        "time_key",
        "time_value",
        "hour",
        "minute",
        "second",
        "is_night",
    )


def _build_dim_date(segments_df: DataFrame) -> DataFrame:
    if "service_date" in segments_df.columns:
        dates = segments_df.select(F.to_date(F.col("service_date"), "yyyyMMdd").alias("date_value"))
    else:
        dates = segments_df.select(
            F.to_date(
                F.to_timestamp(F.col("load_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            ).alias("date_value")
        )

    dates = dates.filter(F.col("date_value").isNotNull()).dropDuplicates()
    dim = dates.withColumn("date_key", F.date_format(F.col("date_value"), "yyyyMMdd").cast(IntegerType()))
    dim = dim.withColumn("year", F.year(F.col("date_value")).cast(IntegerType()))
    dim = dim.withColumn("month", F.month(F.col("date_value")).cast(IntegerType()))
    dim = dim.withColumn("day", F.dayofmonth(F.col("date_value")).cast(IntegerType()))
    return dim.select("date_key", "date_value", "year", "month", "day")

def _build_fact_segments(
    segments_df: DataFrame,
    dim_country: DataFrame,
    dim_operator: DataFrame,
    dim_station: DataFrame,
    dim_route: DataFrame,
    dim_time: DataFrame,
    dim_date: DataFrame,
    mapping_expr: F.Column | None,
) -> DataFrame:
    fact = segments_df.withColumn("country_code", _map_country_col(F.col("country"), mapping_expr))

    fact = fact.join(dim_country.select("country_key", "country_code"), "country_code", "left")
    fact = fact.join(
        dim_operator.select("operator_key", "operator_id"),
        fact["operator"] == dim_operator["operator_id"],
        "left",
    )
    fact = fact.drop(dim_operator["operator_id"])

    fact = fact.join(
        dim_route.select("route_key", "route_id", "operator_id", "country_code"),
        (fact["route_id"] == dim_route["route_id"])
        & (fact["operator"] == dim_route["operator_id"])
        & (fact["country_code"] == dim_route["country_code"]),
        "left",
    ).drop(dim_route["route_id"]).drop(dim_route["operator_id"]).drop(dim_route["country_code"])

    fact = fact.join(
        dim_station.select("station_key", "stop_id", "country_code"),
        (fact["departure_stop_id"] == dim_station["stop_id"])
        & (fact["country_code"] == dim_station["country_code"]),
        "left",
    ).withColumnRenamed("station_key", "departure_station_key").drop(dim_station["stop_id"]).drop(
        dim_station["country_code"]
    )

    fact = fact.join(
        dim_station.select("station_key", "stop_id", "country_code"),
        (fact["arrival_stop_id"] == dim_station["stop_id"])
        & (fact["country_code"] == dim_station["country_code"]),
        "left",
    ).withColumnRenamed("station_key", "arrival_station_key").drop(dim_station["stop_id"]).drop(
        dim_station["country_code"]
    )

    fact = fact.withColumn("departure_time", F.col("departure_time").cast(StringType()))
    fact = fact.withColumn("arrival_time", F.col("arrival_time").cast(StringType()))

    fact = fact.join(
        dim_time.select("time_key", "time_value"),
        fact["departure_time"] == dim_time["time_value"],
        "left",
    ).withColumnRenamed("time_key", "departure_time_key").drop(dim_time["time_value"])

    fact = fact.join(
        dim_time.select("time_key", "time_value"),
        fact["arrival_time"] == dim_time["time_value"],
        "left",
    ).withColumnRenamed("time_key", "arrival_time_key").drop(dim_time["time_value"])

    if "service_date" in fact.columns:
        fact = fact.withColumn("date_value", F.to_date(F.col("service_date"), "yyyyMMdd"))
    else:
        fact = fact.withColumn(
            "date_value",
            F.to_date(F.to_timestamp(F.col("load_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")),
        )
    fact = fact.join(dim_date.select("date_key", "date_value"), "date_value", "left")

    fact = fact.withColumnRenamed("trip_id", "trip_business_id")

    if "distance_km" not in fact.columns:
        fact = fact.withColumn("distance_km", F.lit(None).cast(IntegerType()))

    fact = fact.select(
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
        "distance_km",
    )
    fact = fact.withColumn("fact_trip_key", F.monotonically_increasing_id())
    fact = fact.select(
        "fact_trip_key",
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
        "distance_km",
    )

    return fact


def _read_csv_bytes_to_spark(
    spark: SparkSession,
    data: bytes,
    root_dir: str,
    temp_files: list[str],
) -> DataFrame:
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False, dir=root_dir) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    temp_files.append(tmp_path)
    return spark.read.option("header", "true").option("inferSchema", "false").csv(tmp_path)


def _load_geo(spark: SparkSession, root_dir: str, temp_files: list[str]) -> DataFrame:
    if os.environ.get(SKIP_GEO_ENV, "0").lower() in ("1", "true", "yes"):
        LOGGER.info("Chargement GEO ignore (SKIP_GEO=1).")
        return _spark_empty_df(spark, [])

    local_path = os.environ.get(GEO_PATH_ENV)
    if local_path:
        data = _download_bytes(local_path)
        if data:
            return _read_csv_bytes_to_spark(spark, data, root_dir, temp_files)
        LOGGER.warning("GEO local introuvable ou illisible: %s", local_path)

    data = _download_bytes(GEO_URL)
    if not data:
        return _spark_empty_df(spark, [])
    return _read_csv_bytes_to_spark(spark, data, root_dir, temp_files)


def _load_night_trains(spark: SparkSession, root_dir: str, temp_files: list[str]) -> DataFrame:
    if os.environ.get(SKIP_NIGHT_TRAINS_ENV, "0").lower() in ("1", "true", "yes"):
        LOGGER.info("Chargement night trains ignore (SKIP_NIGHT_TRAINS=1).")
        return _spark_empty_df(spark, [])

    local_path = os.environ.get(NIGHT_TRAINS_PATH_ENV)
    if local_path:
        data = _download_bytes(local_path)
        if data:
            return _read_csv_bytes_to_spark(spark, data, root_dir, temp_files)
        LOGGER.warning("Night trains local introuvable ou illisible: %s", local_path)

    data = _download_bytes(NIGHT_TRAINS_URL)
    if not data:
        return _spark_empty_df(spark, [])
    return _read_csv_bytes_to_spark(spark, data, root_dir, temp_files)


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "obrail"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "143123!"),
    )


def _jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{os.environ.get('PGHOST', 'localhost')}:"
        f"{os.environ.get('PGPORT', '5432')}/"
        f"{os.environ.get('PGDATABASE', 'obrail')}"
    )


def _jdbc_props() -> dict:
    return {
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "143123!"),
        "driver": "org.postgresql.Driver",
    }


def _prepare_df_for_jdbc(df: DataFrame, table_name: str) -> DataFrame:
    table = table_name.split(".")[-1].lower()
    if table == "dim_time" and "time_value" in df.columns:
        df = df.withColumn("time_value", F.to_timestamp(F.col("time_value"), "HH:mm:ss"))
    if table == "trip_stop":
        if "arrival_time" in df.columns:
            df = df.withColumn("arrival_time", F.to_timestamp(F.col("arrival_time"), "HH:mm:ss"))
        if "departure_time" in df.columns:
            df = df.withColumn("departure_time", F.to_timestamp(F.col("departure_time"), "HH:mm:ss"))
    return df


def _split_table_name(table_name: str) -> tuple[str, str]:
    parts = table_name.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return "public", table_name


def _ensure_jdbc_stage_table(table_name: str) -> None:
    schema, table = _split_table_name(table_name)
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        exists = cur.fetchone() is not None
        if not exists:
            columns = [
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type))
                for col, col_type in STAGING_SEGMENTS_SCHEMA
            ]
            cur.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(columns),
                )
            )
            conn.commit()
            return

        cur.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (schema, table, "_stage_id"),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} bigint").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier("_stage_id"),
                )
            )
            conn.commit()
            return

        data_type = (row[0] or "").lower()
        if data_type not in ("bigint", "int8"):
            LOGGER.warning(
                "Type _stage_id inattendu (%s) dans %s.%s, recreation de la colonne.",
                data_type,
                schema,
                table,
            )
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier("_stage_id"),
                )
            )
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} bigint").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier("_stage_id"),
                )
            )
            conn.commit()


def _write_df_jdbc(df: DataFrame, table_name: str) -> None:
    url = _jdbc_url()
    props = _jdbc_props()
    df = _prepare_df_for_jdbc(df, table_name)
    df.write.jdbc(url=url, table=table_name, mode="append", properties=props)


def _stage_segments_to_jdbc(df: DataFrame, table_name: str) -> None:
    url = _jdbc_url()
    batchsize = os.environ.get(STAGING_JDBC_BATCHSIZE_ENV, "5000")
    write_partitions = os.environ.get(STAGING_JDBC_WRITE_PARTITIONS_ENV)
    if write_partitions and write_partitions.isdigit():
        target = int(write_partitions)
        if target > 0:
            df = df.coalesce(target)
    df = df.withColumn("_stage_id", F.monotonically_increasing_id().cast(LongType()))
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("driver", "org.postgresql.Driver")
        .option("user", os.environ.get("PGUSER", "postgres"))
        .option("password", os.environ.get("PGPASSWORD", "143123!"))
        .option("batchsize", batchsize)
        .mode("append")
        .save()
    )


def _read_staged_segments_jdbc(
    spark: SparkSession,
    table_name: str,
    num_partitions: int,
) -> DataFrame | None:
    lower = None
    upper = None
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT min(_stage_id), max(_stage_id) FROM {table_name}")
            row = cur.fetchone()
            if row:
                lower, upper = row[0], row[1]
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Lecture bornes staging JDBC impossible (%s)", exc)

    if lower is None or upper is None:
        return None
    if lower == upper:
        num_partitions = 1
    return spark.read.jdbc(
        url=_jdbc_url(),
        table=table_name,
        column="_stage_id",
        lowerBound=int(lower),
        upperBound=int(upper),
        numPartitions=num_partitions,
        properties=_jdbc_props(),
    )


def run_stream_etl(country_codes: set[str] | None = None) -> None:
    _setup_logger()
    spark = _build_spark_session()
    low_mem = os.environ.get(LOW_MEMORY_MODE_ENV, "1").lower() in ("1", "true", "yes")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    tmp_base = os.path.join(project_root, "data", "tmp")
    if os.name == "nt":
        short_base = (
            os.environ.get("OBRAIL_TMP_DIR")
            or os.environ.get("SPARK_LOCAL_DIR")
            or "C:\\spark-tmp"
        )
        for sep in (",", ";"):
            if sep in short_base:
                short_base = short_base.split(sep)[0]
                break
        tmp_base = os.path.join(short_base, "obrail")
    os.makedirs(tmp_base, exist_ok=True)
    tmp_root = tempfile.mkdtemp(prefix=f"gtfs_root_{uuid.uuid4().hex}_", dir=tmp_base)
    staging_dir = os.environ.get(STAGING_DIR_ENV)
    if not staging_dir:
        staging_dir = os.path.join(tmp_root, "staging")
    os.makedirs(staging_dir, exist_ok=True)
    staging_mode_env = os.environ.get(STAGING_MODE_ENV)
    staging_mode = (staging_mode_env or "parquet").strip().lower()
    if os.name == "nt" and staging_mode_env is None:
        staging_mode = "jdbc"
        LOGGER.info(
            "Windows detecte sans %s: bascule staging JDBC pour eviter les libs Hadoop natives.",
            STAGING_MODE_ENV,
        )
    staging_is_jdbc = staging_mode in ("jdbc", "postgres", "pg")
    use_staging = staging_mode in ("1", "true", "yes", "parquet") or staging_is_jdbc
    segments_stage_path = os.path.join(staging_dir, "segments")
    segments_stage_table = os.environ.get(STAGING_JDBC_TABLE_ENV, "obrail.stg_segments")
    staging_jdbc_partitions = int(os.environ.get(STAGING_JDBC_PARTITIONS_ENV, "32"))
    checkpoint_mode_default = "off" if staging_is_jdbc else ("parquet" if low_mem else "off")
    checkpoint_mode = os.environ.get(CHECKPOINT_MODE_ENV, checkpoint_mode_default).strip().lower()
    use_checkpoint = checkpoint_mode in ("1", "true", "yes", "parquet")
    checkpoint_dir = os.environ.get(CHECKPOINT_DIR_ENV)
    if not checkpoint_dir:
        checkpoint_dir = os.path.join(tmp_root, "checkpoint")
    checkpoint_path = os.path.join(checkpoint_dir, "segments")
    checkpoint_cleanup = os.environ.get(
        CHECKPOINT_CLEANUP_ENV,
        "1" if low_mem else "0",
    ).lower() in ("1", "true", "yes")
    download_workers = int(os.environ.get(GTFS_DOWNLOAD_WORKERS_ENV, "1") or "1")
    feed_workers = int(os.environ.get(GTFS_FEED_PROCESS_WORKERS_ENV, "1") or "1")
    raw_cache_dir = os.environ.get(GTFS_CACHE_DIR_ENV)
    cache_dir = None
    if raw_cache_dir and raw_cache_dir.lower() not in ("0", "false", "no"):
        cache_dir = os.path.abspath(raw_cache_dir)
    if download_workers > 1 and not cache_dir:
        cache_dir = os.path.join(tmp_base, "gtfs_cache")
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
    raw_segments_cache_dir = os.environ.get(GTFS_SEGMENTS_CACHE_DIR_ENV)
    segments_cache_dir = None
    if raw_segments_cache_dir and raw_segments_cache_dir.lower() not in ("0", "false", "no"):
        if raw_segments_cache_dir.lower() in ("1", "true", "yes"):
            segments_cache_dir = os.path.join(tmp_base, "segments_cache")
        else:
            segments_cache_dir = os.path.abspath(raw_segments_cache_dir)
        os.makedirs(segments_cache_dir, exist_ok=True)
    segments_cache_ttl_hours = int(os.environ.get(GTFS_SEGMENTS_CACHE_TTL_HOURS_ENV, "0") or "0")
    batch_by_country = os.environ.get(BATCH_BY_COUNTRY_ENV, "1").lower() in ("1", "true", "yes")
    skip_http_403 = os.environ.get(SKIP_FEEDS_HTTP_403_ENV, "1").lower() in ("1", "true", "yes")
    skip_timeout = os.environ.get(SKIP_FEEDS_TIMEOUT_ENV, "1").lower() in ("1", "true", "yes")
    temp_csv_files: list[str] = []
    blocked_urls: dict[str, str] = {}
    staged_any = False

    try:
        with _timed("sources:resolve"):
            target_codes = country_codes or _parse_country_codes(os.environ.get("TARGET_COUNTRIES"))
            if not target_codes:
                target_codes = DEFAULT_TARGET_COUNTRIES
            max_per_country = int(os.environ.get("MAX_FEEDS_PER_COUNTRY", "3"))

            single_sources = _load_single_gtfs_source()
            if single_sources:
                sources = single_sources
            else:
                use_static = os.environ.get(USE_STATIC_GTFS_SOURCES_ENV, "1").lower() not in (
                    "0",
                    "false",
                    "no",
                )
                sources = []
                if use_static:
                    sources = list(GTFS_SOURCES)
                    filter_static = os.environ.get(GTFS_SOURCES_FILTER_BY_COUNTRY_ENV, "0").lower() in (
                        "1",
                        "true",
                        "yes",
                    )
                    if filter_static:
                        sources = [
                            src for src in sources if _match_country(target_codes, src.get("country"))
                        ]

                local_dir = os.environ.get(GTFS_LOCAL_DIR_ENV)
                local_country = os.environ.get(GTFS_LOCAL_COUNTRY_ENV)
                sources.extend(_load_local_gtfs_sources(local_dir, local_country))

            use_catalog = os.environ.get(USE_MOBILITY_DATABASE_SOURCES_ENV, "1").lower() not in (
                "0",
                "false",
                "no",
            )
            if use_catalog:
                priority_sources = get_mobility_database_sources(
                    country_codes={c.upper() for c in target_codes},
                    max_per_country=max_per_country,
                )
                sources.extend(priority_sources)
            else:
                LOGGER.info(
                    "Mobility Database desactive (USE_MOBILITY_DATABASE_SOURCES=0): "
                    "uniquement sources statiques/locales."
                )

            max_total = int(os.environ.get("MAX_TOTAL_FEEDS", "0"))
            if max_total > 0:
                sources = sources[:max_total]

        LOGGER.info("Sources GTFS en streaming (Spark): %s", sources)
        if use_staging:
            if staging_is_jdbc:
                LOGGER.info("Staging JDBC actif: %s", segments_stage_table)
                try:
                    _ensure_jdbc_stage_table(segments_stage_table)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Initialisation staging JDBC impossible (%s)", exc)
                truncate_stage = os.environ.get(STAGING_JDBC_TRUNCATE_ENV, "1").lower() in (
                    "1",
                    "true",
                    "yes",
                )
                if truncate_stage:
                    try:
                        with _get_conn() as conn, conn.cursor() as cur:
                            cur.execute(f"TRUNCATE TABLE {segments_stage_table}")
                            conn.commit()
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("Impossible de TRUNCATE %s (%s)", segments_stage_table, exc)
            else:
                LOGGER.info("Staging parquet actif: %s", segments_stage_path)
        if use_checkpoint:
            LOGGER.info("Checkpoint parquet actif: %s", checkpoint_path)

        if batch_by_country:
            grouped_sources: dict[str, list[dict]] = {}
            for src in sources:
                key = str(src.get("country") or "unknown").lower()
                grouped_sources.setdefault(key, []).append(src)
            source_groups = list(grouped_sources.items())
        else:
            source_groups = [("all", sources)]

        segments_df = None
        trip_stops_df = None
        skip_trip_stops = os.environ.get(SKIP_TRIP_STOPS_ENV, "1").lower() in (
            "1",
            "true",
            "yes",
        )
        if not skip_trip_stops:
            LOGGER.info("Trip stops desactives par defaut pour limiter la memoire. (SKIP_TRIP_STOPS=1)")
            skip_trip_stops = True

        if feed_workers > 1 and (staging_is_jdbc or not use_staging):
            LOGGER.warning(
                "Parallele feeds requiert staging parquet. Desactivation (workers=%s).",
                feed_workers,
            )
            feed_workers = 1
        if feed_workers > 1 and not skip_trip_stops:
            LOGGER.warning(
                "Parallele feeds incompatible avec trip_stops. Desactivation (workers=%s).",
                feed_workers,
            )
            feed_workers = 1

        staged_paths: list[str] = []
        for group_key, group_sources in source_groups:
            if batch_by_country:
                LOGGER.info("Batch country %s: %s feeds", group_key, len(group_sources))

            download_cache: dict[str, tuple[str | None, str | None]] = {}
            if download_workers > 1 and group_sources:
                LOGGER.info(
                    "Prefetch GTFS: %s feeds avec %s workers (cache=%s).",
                    len(group_sources),
                    download_workers,
                    cache_dir or "off",
                )
                with _timed(f"download:prefetch {group_key}"):
                    with ThreadPoolExecutor(max_workers=download_workers) as executor:
                        future_map = {}
                        for src in group_sources:
                            url = src.get("url")
                            if not url:
                                continue
                            if url in blocked_urls:
                                continue
                            future = executor.submit(
                                _download_gtfs_to_cache,
                                url,
                                src.get("fallback_url"),
                                cache_dir,
                            )
                            future_map[future] = url
                        for future in as_completed(future_map):
                            url = future_map[future]
                            try:
                                cache_path, err = future.result()
                            except Exception as exc:  # noqa: BLE001
                                LOGGER.warning("Prefetch impossible: %s (%s)", url, exc)
                                download_cache[url] = (None, "prefetch_error")
                                continue
                            download_cache[url] = (cache_path, err)

            feed_output_paths: list[str] = []

            def _process_feed(
                src: dict,
            ) -> tuple[str | None, str | None, str | None, bool, DataFrame | None]:
                url = src.get("url")
                country = src.get("country")
                operator = src.get("operator")
                feed_label = operator or country or "unknown"
                if not url:
                    return None, None, None, False, None
                if url in blocked_urls:
                    return None, url, blocked_urls[url], False, None
                LOGGER.info("Telechargement GTFS: %s", url)
                fallback_url = src.get("fallback_url")
                content = None
                err = None
                cache_path = None
                cached = download_cache.get(url)
                if cached is not None:
                    cache_path, err = cached
                    if cache_path:
                        LOGGER.info("Cache ZIP hit: %s", feed_label)
                        content = _read_cached_bytes(cache_path)
                        if content is None:
                            err = "cache_read_error"
                elif cache_dir:
                    with _timed(f"feed:download {operator or country or 'unknown'}"):
                        cache_path, err = _download_gtfs_to_cache(url, fallback_url, cache_dir)
                    if cache_path:
                        LOGGER.info("Cache ZIP miss: %s", feed_label)
                        content = _read_cached_bytes(cache_path)
                        if content is None:
                            err = "cache_read_error"
                else:
                    with _timed(f"feed:download {operator or country or 'unknown'}"):
                        content, err = _download_gtfs_bytes(url, fallback_url=fallback_url)
                if err:
                    return None, url, err, False, None

                segments_cache_path = None
                cache_lock = None
                if segments_cache_dir and cache_path:
                    cache_key = _segments_cache_key(url, cache_path)
                    segments_cache_path = _segments_cache_path(segments_cache_dir, cache_key)
                    if _segments_cache_valid(segments_cache_path, segments_cache_ttl_hours):
                        LOGGER.info("Cache segments hit: %s", feed_label)
                        return segments_cache_path, None, None, True, None
                    LOGGER.info("Cache segments miss: %s", feed_label)
                    cache_lock = _acquire_cache_lock(segments_cache_path)
                    if cache_lock is None:
                        LOGGER.info("Cache segments verrouille: %s", feed_label)
                        segments_cache_path = None

                if use_staging:
                    if staging_is_jdbc:
                        _release_cache_lock(cache_lock)
                        cache_lock = None
                        with _timed(f"feed:stage_jdbc {operator or country or 'unknown'}"):
                            _stage_segments_from_zip_jdbc(
                                spark,
                                content,
                                country=country,
                                operator=operator,
                                root_dir=tmp_root,
                                table_name=segments_stage_table,
                            )
                        return None, None, None, True, None
                    stage_path = segments_cache_path
                    if not stage_path:
                        feed_key = hashlib.sha256(
                            f"{url}|{operator or ''}|{country or ''}".encode("utf-8")
                        ).hexdigest()[:12]
                        stage_path = os.path.join(staging_dir, "feeds", feed_key)
                    os.makedirs(stage_path, exist_ok=True)
                    try:
                        with _timed(f"feed:stage_parquet {operator or country or 'unknown'}"):
                            _stage_segments_from_zip(
                                spark,
                                content,
                                country=country,
                                operator=operator,
                                root_dir=tmp_root,
                                stage_path=stage_path,
                                write_mode="overwrite",
                            )
                    finally:
                        _release_cache_lock(cache_lock)
                    return stage_path, None, None, True, None

                with _timed(f"feed:extract {operator or country or 'unknown'}"):
                    seg_df = _extract_segments_from_zip(
                        spark,
                        content,
                        country=country,
                        operator=operator,
                        root_dir=tmp_root,
                    )
                if segments_cache_path:
                    try:
                        seg_df.write.mode("overwrite").parquet(segments_cache_path)
                    finally:
                        _release_cache_lock(cache_lock)
                    return segments_cache_path, None, None, True, None
                _release_cache_lock(cache_lock)
                return None, None, None, False, seg_df

            if feed_workers > 1:
                with ThreadPoolExecutor(max_workers=feed_workers) as executor:
                    future_map = {executor.submit(_process_feed, src): src for src in group_sources}
                    for future in as_completed(future_map):
                        try:
                            stage_path, failed_url, err, staged_flag, _ = future.result()
                        except Exception as exc:  # noqa: BLE001
                            LOGGER.warning("Traitement feed en erreur: %s", exc)
                            continue
                        if err and failed_url:
                            blocked = False
                            if err == "http_403" and skip_http_403:
                                blocked = True
                            if err == "timeout" and skip_timeout:
                                blocked = True
                            if blocked:
                                blocked_urls[failed_url] = err
                            LOGGER.warning(
                                "Telechargement impossible: %s (%s)%s",
                                failed_url,
                                err,
                                " [exclu]" if blocked else "",
                            )
                            continue
                        if stage_path:
                            feed_output_paths.append(stage_path)
                        if staged_flag:
                            staged_any = True
            else:
                for src in group_sources:
                    stage_path, failed_url, err, staged_flag, seg_df = _process_feed(src)
                    if err and failed_url:
                        blocked = False
                        if err == "http_403" and skip_http_403:
                            blocked = True
                        if err == "timeout" and skip_timeout:
                            blocked = True
                        if blocked:
                            blocked_urls[failed_url] = err
                        LOGGER.warning(
                            "Telechargement impossible: %s (%s)%s",
                            failed_url,
                            err,
                            " [exclu]" if blocked else "",
                        )
                        continue
                    if stage_path:
                        feed_output_paths.append(stage_path)
                    if staged_flag:
                        staged_any = True
                    if seg_df is None and stage_path and not use_staging:
                        seg_df = spark.read.parquet(stage_path)
                    if seg_df is not None:
                        if segments_df is None:
                            segments_df = seg_df
                        else:
                            segments_df = segments_df.unionByName(seg_df, allowMissingColumns=True)

            if not skip_trip_stops:
                for src in group_sources:
                    url = src.get("url")
                    country = src.get("country")
                    operator = src.get("operator")
                    if not url or url in blocked_urls:
                        continue
                    fallback_url = src.get("fallback_url")
                    content = None
                    err = None
                    cache_path = None
                    cached = download_cache.get(url)
                    if cached is not None:
                        cache_path, err = cached
                        if cache_path:
                            content = _read_cached_bytes(cache_path)
                            if content is None:
                                err = "cache_read_error"
                    elif cache_dir:
                        cache_path, err = _download_gtfs_to_cache(url, fallback_url, cache_dir)
                        if cache_path:
                            content = _read_cached_bytes(cache_path)
                            if content is None:
                                err = "cache_read_error"
                    else:
                        content, err = _download_gtfs_bytes(url, fallback_url=fallback_url)
                    if err or content is None:
                        continue
                    with _timed(f"feed:trip_stops {operator or country or 'unknown'}"):
                        stops_df = _extract_trip_stops_from_zip(
                            spark,
                            content,
                            country=country,
                            operator=operator,
                            root_dir=tmp_root,
                        )
                    if trip_stops_df is None:
                        trip_stops_df = stops_df
                    else:
                        trip_stops_df = trip_stops_df.unionByName(stops_df, allowMissingColumns=True)

            if feed_output_paths:
                staged_paths.extend(feed_output_paths)

            if batch_by_country:
                spark.catalog.clearCache()
                gc.collect()
                try:
                    spark._jvm.System.gc()
                except Exception:
                    pass

        if use_staging:
            if staging_is_jdbc:
                if staged_any:
                    segments_df = _read_staged_segments_jdbc(
                        spark,
                        segments_stage_table,
                        staging_jdbc_partitions,
                    )
            elif staged_paths:
                segments_df = spark.read.parquet(*staged_paths)
            elif segments_df is None and os.path.exists(segments_stage_path):
                segments_df = spark.read.parquet(segments_stage_path)

        if segments_df is None or (not use_staging and _df_is_empty(segments_df)):
            LOGGER.warning("Aucun segment GTFS extrait.")
            return
        if "_stage_id" in segments_df.columns:
            segments_df = segments_df.drop("_stage_id")

        with _timed("segments:transform"):
            segments_df = transform_trip_segments(segments_df)
        if use_checkpoint:
            os.makedirs(checkpoint_dir, exist_ok=True)
            checkpoint_df = segments_df
            checkpoint_coalesce = os.environ.get(CHECKPOINT_COALESCE_ENV)
            if checkpoint_coalesce and checkpoint_coalesce.isdigit():
                target = int(checkpoint_coalesce)
                if target > 0:
                    checkpoint_df = checkpoint_df.coalesce(target)
            with _timed("segments:checkpoint_write"):
                checkpoint_df.write.mode("overwrite").parquet(checkpoint_path)
            segments_df = spark.read.parquet(checkpoint_path)
            if use_staging and checkpoint_cleanup and _is_within_dir(segments_stage_path, tmp_root):
                shutil.rmtree(segments_stage_path, ignore_errors=True)
        if os.environ.get(LOW_MEMORY_MODE_ENV, "1").lower() in ("1", "true", "yes"):
            segments_df = segments_df.persist(StorageLevel.DISK_ONLY)
        else:
            segments_df = segments_df.cache()

        if skip_trip_stops:
            LOGGER.info("Trip stops ignores (SKIP_TRIP_STOPS=1).")
            trip_stops_df = None
        elif trip_stops_df is not None and not _df_is_empty(trip_stops_df):
            trip_stops_df = trip_stops_df.withColumn(
                "date_value",
                F.to_date(F.col("service_date"), "yyyyMMdd"),
            ).drop("service_date")
            trip_stops_df = trip_stops_df.dropna(subset=["trip_id", "stop_id"])
            trip_stops_df = trip_stops_df.dropDuplicates(["trip_id", "date_value", "stop_sequence"])
            trip_stops_df = trip_stops_df.withColumn(
                "trip_stop_key",
                F.monotonically_increasing_id().cast(LongType()),
            )
            trip_stops_df = trip_stops_df.select(
                "trip_stop_key",
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
                "date_value",
            )

        with _timed("dims:build"):
            night_df = transform_night_trains(_load_night_trains(spark, tmp_root, temp_csv_files))
            geo_df = _load_geo(spark, tmp_root, temp_csv_files)

            if _df_is_empty(geo_df):
                LOGGER.warning("GEO vide: construction de dim_country depuis les segments.")
                dim_country = _build_dim_country_from_segments(segments_df)
            else:
                dim_country = _build_dim_country(geo_df)

            country_mapping = _build_country_mapping(dim_country)
            mapping_expr = _country_map_expr(country_mapping)

            dim_operator = _build_dim_operator(segments_df, night_df, mapping_expr)
            dim_station = _build_dim_station(segments_df, mapping_expr)
            dim_route = _build_dim_route(segments_df, mapping_expr)
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
                mapping_expr,
            )

        schema_path = os.path.join(project_root, "data", "scripts", "mart", "schema.sql")

        with _get_conn() as conn:
            with conn.cursor() as cur:
                if os.path.exists(schema_path):
                    with open(schema_path, "r", encoding="utf-8") as schema_file:
                        cur.execute(schema_file.read())

                cur.execute(
                    "TRUNCATE obrail.fact_trip_segment, obrail.dim_time, obrail.dim_date, "
                    "obrail.dim_route, obrail.dim_station, obrail.dim_operator, obrail.dim_country, "
                    "obrail.trip_stop CASCADE"
                )

            conn.commit()

        with _timed("db:write"):
            _write_df_jdbc(dim_country, "obrail.dim_country")
            _write_df_jdbc(dim_operator, "obrail.dim_operator")
            _write_df_jdbc(dim_station, "obrail.dim_station")
            _write_df_jdbc(dim_route, "obrail.dim_route")
            _write_df_jdbc(dim_time, "obrail.dim_time")
            _write_df_jdbc(dim_date, "obrail.dim_date")
            _write_df_jdbc(fact_segments, "obrail.fact_trip_segment")
            if trip_stops_df is not None and not _df_is_empty(trip_stops_df):
                _write_df_jdbc(trip_stops_df, "obrail.trip_stop")

        LOGGER.info("ETL Spark termine a %s", datetime.now(timezone.utc).isoformat())
    finally:
        for path in temp_csv_files:
            try:
                os.remove(path)
            except OSError:
                pass
        if blocked_urls:
            LOGGER.warning("GTFS exclus (403/timeout): %s", blocked_urls)
        shutil.rmtree(tmp_root, ignore_errors=True)
        try:
            spark.stop()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Spark stop failed: %s", exc)


if __name__ == "__main__":
    run_stream_etl()
