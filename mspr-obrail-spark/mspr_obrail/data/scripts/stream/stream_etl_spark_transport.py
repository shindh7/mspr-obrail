"""
Spark ETL for trains and planes (minimal data mart).

Goals:
- Download input files provided by the user (GTFS ZIP/dir or flights CSV)
- Keep only trains and planes
- Keep only departure/arrival stations
- Compute distance with GPS coords (Haversine)
- Drop invalid coords and trips under MIN_DISTANCE_KM
- Deduplicate trips, including reverse direction
- Build SQL tables: vehicule, station, trajet

Inputs:
- INPUT_FILES: comma-separated list of URLs or local paths
- INPUT_FILE_LIST: optional text file with one path/URL per line

Outputs:
- Writes into PostgreSQL schema (default: obrail_transport)
"""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
import tempfile
import zipfile
import sys
from datetime import datetime
import time
from urllib.parse import urlparse

import psycopg2
import requests
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

LOGGER = logging.getLogger("stream_etl_spark_transport")

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

INPUT_FILES_ENV = "INPUT_FILES"
INPUT_FILE_LIST_ENV = "INPUT_FILE_LIST"
MIN_DISTANCE_KM_ENV = "MIN_DISTANCE_KM"
DISTANCE_FACTOR_TRAIN_ENV = "DISTANCE_FACTOR_TRAIN"
CO2_PER_KM_TRAIN_ENV = "CO2_PER_KM_TRAIN"
CO2_PER_KM_AIR_ENV = "CO2_PER_KM_AIR"
ALLOWED_TRAIN_ROUTE_TYPES_ENV = "ALLOWED_TRAIN_ROUTE_TYPES"
ALLOWED_AIR_ROUTE_TYPES_ENV = "ALLOWED_AIR_ROUTE_TYPES"
PGSCHEMA_ENV = "PGSCHEMA"
JDBC_WRITE_PARTITIONS_ENV = "JDBC_WRITE_PARTITIONS"
JDBC_BATCH_SIZE_ENV = "JDBC_BATCH_SIZE"
JDBC_RETRY_ON_ERROR_ENV = "JDBC_RETRY_ON_ERROR"
JDBC_FALLBACK_PARTITIONS_ENV = "JDBC_FALLBACK_PARTITIONS"
JDBC_FALLBACK_BATCH_SIZE_ENV = "JDBC_FALLBACK_BATCH_SIZE"
SPARK_SHUFFLE_PARTITIONS_ENV = "SPARK_SHUFFLE_PARTITIONS"
SPARK_LOG_LEVEL_ENV = "SPARK_LOG_LEVEL"
SPARK_DRIVER_MEMORY_ENV = "SPARK_DRIVER_MEMORY"
SPARK_EXECUTOR_MEMORY_ENV = "SPARK_EXECUTOR_MEMORY"
SPARK_DRIVER_MEMORY_OVERHEAD_ENV = "SPARK_DRIVER_MEMORY_OVERHEAD"
SPARK_EXECUTOR_MEMORY_OVERHEAD_ENV = "SPARK_EXECUTOR_MEMORY_OVERHEAD"
SPARK_LOCAL_DIR_ENV = "SPARK_LOCAL_DIR"
ETL_LOG_FILE_ENV = "ETL_LOG_FILE"
DOWNLOAD_TIMEOUT_ENV = "DOWNLOAD_TIMEOUT"
DOWNLOAD_RETRIES_ENV = "DOWNLOAD_RETRIES"
DOWNLOAD_CHUNK_MB_ENV = "DOWNLOAD_CHUNK_MB"
DEFAULT_COUNTRY_CODE_ENV = "DEFAULT_COUNTRY_CODE"
GTFS_DEFAULT_COUNTRY_ENV = "GTFS_DEFAULT_COUNTRY"
FLIGHTS_DEFAULT_COUNTRY_ENV = "FLIGHTS_DEFAULT_COUNTRY"
GTFS_INCLUDE_STATIC_SOURCES_ENV = "GTFS_INCLUDE_STATIC_SOURCES"
GTFS_VALIDATE_URLS_ENV = "GTFS_VALIDATE_URLS"
GTFS_VALIDATE_TIMEOUT_ENV = "GTFS_VALIDATE_TIMEOUT"
GTFS_VALIDATE_MAX_CANDIDATES_ENV = "GTFS_VALIDATE_MAX_CANDIDATES"
GTFS_IGNORELIST_PATH_ENV = "GTFS_IGNORELIST_PATH"
GTFS_IGNORELIST_ENABLED_ENV = "GTFS_IGNORELIST_ENABLED"
GTFS_IGNORELIST_RESET_ENV = "GTFS_IGNORELIST_RESET"
GTFS_FILL_MISSING_COUNTRY_ENV = "GTFS_FILL_MISSING_COUNTRY"
GTFS_VALIDATE_COUNTRY_BBOX_ENV = "GTFS_VALIDATE_COUNTRY_BBOX"
GTFS_COUNTRY_BBOX_MARGIN_ENV = "GTFS_COUNTRY_BBOX_MARGIN"
ETL_SNAPSHOT_DIR_ENV = "ETL_SNAPSHOT_DIR"
ETL_SNAPSHOT_WRITE_ENV = "ETL_SNAPSHOT_WRITE"
ETL_SNAPSHOT_LOAD_ENV = "ETL_SNAPSHOT_LOAD"
ETL_SNAPSHOT_ID_ENV = "ETL_SNAPSHOT_ID"
ETL_TRUNCATE_ENV = "ETL_TRUNCATE"

GTFS_CATALOG_URL = "https://files.mobilitydatabase.org/feeds_v2.csv" #"C:\\2_EPSI\\MSPR\\mspr_code\\feed_v2_eu27_gtfs_active.csv"
GTFS_MAX_SOURCES = 3
GTFS_COUNTRY_CODES = {
    "FR",
    "DE",
    "GB",
    "IT",
    "ES",
    "NL",
    "FI",
    "BE",
    #"CH",
    "AT",
    "PL",
    "SE",
    "NO",
    "DK",
}

DEFAULT_ALLOWED_TRAIN_ROUTE_TYPES = "2,100-117"
DEFAULT_ALLOWED_AIR_ROUTE_TYPES = "1100-1107"
DEFAULT_MIN_DISTANCE_KM = 100
DEFAULT_DISTANCE_FACTOR_TRAIN = 1.3
DEFAULT_CO2_PER_KM_TRAIN = 0.0
DEFAULT_CO2_PER_KM_AIR = 0.0
DEFAULT_SCHEMA = "obrail_transport"
COORD_ROUND_DECIMALS = 4
EARTH_RADIUS_KM = 6371.0

COUNTRY_NAME_ALIASES: dict[str, str] = {
    "fr": "FR",
    "france": "FR",
    "french republic": "FR",
    "gb": "GB",
    "uk": "GB",
    "united kingdom": "GB",
    "great britain": "GB",
    "britain": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "de": "DE",
    "germany": "DE",
    "deutschland": "DE",
    "fi": "FI",
    "finland": "FI",
    "cz": "CZ",
    "czechia": "CZ",
    "czech republic": "CZ",
    "es": "ES",
    "spain": "ES",
    "it": "IT",
    "italy": "IT",
    "nl": "NL",
    "netherlands": "NL",
    "be": "BE",
    "belgium": "BE",
    "ch": "CH",
    "switzerland": "CH",
    "at": "AT",
    "austria": "AT",
    "pl": "PL",
    "poland": "PL",
    "se": "SE",
    "sweden": "SE",
    "no": "NO",
    "norway": "NO",
    "dk": "DK",
    "denmark": "DK",
    "pt": "PT",
    "portugal": "PT",
    "ie": "IE",
    "ireland": "IE",
    "lu": "LU",
    "luxembourg": "LU",
    "gr": "GR",
    "greece": "GR",
    "ro": "RO",
    "romania": "RO",
    "bg": "BG",
    "bulgaria": "BG",
    "hr": "HR",
    "croatia": "HR",
    "sk": "SK",
    "slovakia": "SK",
    "si": "SI",
    "slovenia": "SI",
    "hu": "HU",
    "hungary": "HU",
    "lt": "LT",
    "lithuania": "LT",
    "lv": "LV",
    "latvia": "LV",
    "ee": "EE",
    "estonia": "EE",
}

COUNTRY_BBOXES: dict[str, tuple[float, float, float, float]] = {
    "AT": (46.2, 49.3, 9.3, 17.5),
    "BE": (49.4, 51.7, 2.4, 6.5),
    "DE": (47.1, 55.2, 5.6, 15.6),
    "DK": (54.3, 57.9, 8.0, 15.3),
    "ES": (35.8, 43.9, -9.7, 4.6),
    "FI": (59.3, 70.3, 19.0, 32.1),
    "FR": (41.0, 51.7, -5.6, 9.9),
    "GB": (49.7, 59.6, -8.8, 2.2),
    "IT": (36.3, 47.2, 6.4, 19.2),
    "NL": (50.7, 53.7, 3.2, 7.3),
    "NO": (57.6, 71.6, 4.0, 31.5),
    "PL": (49.0, 55.2, 14.0, 24.3),
    "SE": (55.0, 69.2, 10.4, 24.8),
    "CZ": (48.5, 51.2, 12.0, 18.9),
    "CH": (45.7, 47.9, 5.8, 10.6),
    "PT": (36.8, 42.2, -9.6, -6.2),
    "IE": (51.3, 55.5, -10.6, -5.4),
}


def _setup_logger() -> None:
    if LOGGER.handlers:
        return
    LOGGER.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    log_path = os.environ.get(ETL_LOG_FILE_ENV)
    if not log_path:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        log_dir = os.path.join(project_root, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "transport_etl.log")
    try:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        LOGGER.addHandler(file_handler)
        LOGGER.info("ETL log file: %s", log_path)
    except OSError as exc:
        LOGGER.warning("ETL log file setup failed: %s (%s)", log_path, exc)


def _parse_list_env(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "y", "on"):
        return True
    if value in ("0", "false", "no", "n", "off"):
        return False
    return default


def _country_bbox_margin() -> float:
    raw = os.environ.get(GTFS_COUNTRY_BBOX_MARGIN_ENV)
    try:
        return float(raw) if raw else 0.5
    except ValueError:
        return 0.5


def _apply_country_bbox(stops: DataFrame) -> DataFrame:
    if not _truthy_env(GTFS_VALIDATE_COUNTRY_BBOX_ENV, False):
        return stops
    if "stop_country" not in stops.columns:
        return stops
    margin = _country_bbox_margin()
    stops = stops.withColumn("stop_lat", F.col("stop_lat").cast(DoubleType()))
    stops = stops.withColumn("stop_lon", F.col("stop_lon").cast(DoubleType()))
    cond = None
    for code, (min_lat, max_lat, min_lon, max_lon) in COUNTRY_BBOXES.items():
        within = (
            (F.col("stop_country") == F.lit(code))
            & F.col("stop_lat").between(min_lat - margin, max_lat + margin)
            & F.col("stop_lon").between(min_lon - margin, max_lon + margin)
        )
        cond = within if cond is None else (cond | within)
    if cond is None:
        return stops
    in_map = F.col("stop_country").isin(list(COUNTRY_BBOXES.keys()))
    stops = stops.withColumn(
        "stop_country",
        F.when(in_map & (~cond), F.lit(None)).otherwise(F.col("stop_country")),
    )
    return stops


def _default_ignorelist_path() -> str:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    return os.path.join(project_root, "data", "scripts", "stream", "gtfs_ignorelist.txt")


def _default_snapshot_dir() -> str:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    return os.path.join(project_root, "data", "snapshots")


def _snapshot_id() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _next_snapshot_id(base_id: str, snapshot_dir: str) -> str:
    if not base_id:
        return _snapshot_id()
    candidate = base_id
    if not os.path.exists(os.path.join(snapshot_dir, candidate)):
        return candidate
    suffix = 1
    while True:
        candidate = f"{base_id}_{suffix:02d}"
        if not os.path.exists(os.path.join(snapshot_dir, candidate)):
            return candidate
        suffix += 1


def _load_ignorelist(path: str) -> set[str]:
    if not path or not os.path.exists(path):
        return set()
    items: set[str] = set()
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                value = line.strip()
                if not value or value.startswith("#"):
                    continue
                items.add(value)
    except OSError as exc:
        LOGGER.warning("Ignorelist read failed: %s (%s)", path, exc)
        return set()
    return items


def _save_ignorelist(path: str, items: set[str]) -> None:
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            for value in sorted(items):
                handle.write(f"{value}\n")
    except OSError as exc:
        LOGGER.warning("Ignorelist write failed: %s (%s)", path, exc)


def _default_country_code(kind: str) -> str | None:
    specific_env = (
        GTFS_DEFAULT_COUNTRY_ENV if kind == "gtfs" else FLIGHTS_DEFAULT_COUNTRY_ENV
    )
    raw = os.environ.get(specific_env) or os.environ.get(DEFAULT_COUNTRY_CODE_ENV)
    if not raw:
        return None
    code = raw.strip().upper()
    return code or None


def _split_input_spec(raw: str) -> tuple[str, str | None]:
    if "|" in raw:
        base, code = raw.rsplit("|", 1)
        code = code.strip()
        if 2 <= len(code) <= 3 and code.isalpha():
            return base.strip(), code.upper()
    return raw, None


def _normalize_country_key(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip().lower()
    if not text or text in ("nan", "null", "none"):
        return ""
    text = text.replace("_", " ").replace("-", " ").replace(".", " ")
    return " ".join(text.split())


def _country_name_to_code(name: str | None, name_map: dict[str, str] | None = None) -> str | None:
    if not name:
        return None
    text = name.strip()
    if not text:
        return None
    if 2 <= len(text) <= 3 and text.isalpha():
        return text.upper()
    key = _normalize_country_key(text)
    if not key:
        return None
    if name_map and key in name_map:
        return name_map[key]
    return COUNTRY_NAME_ALIASES.get(key)


def _load_gtfs_catalog_country_map() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    path = (
        os.environ.get("GTFS_CATALOG_URL")
        or os.environ.get("MOBILITY_DATABASE_CATALOG_PATH")
        or os.environ.get("MOBILITY_DATABASE_CATALOG_URL")
        or GTFS_CATALOG_URL
    )
    if not path:
        return {}, {}, {}

    content: str | None = None
    if path.startswith("http://") or path.startswith("https://"):
        try:
            response = requests.get(path, timeout=60)
            response.raise_for_status()
            content = response.text
        except requests.RequestException as exc:
            LOGGER.warning("GTFS catalog download failed: %s (%s)", path, exc)
            return {}, {}, {}
    else:
        if not os.path.exists(path):
            LOGGER.warning("GTFS catalog not found: %s", path)
            return {}, {}, {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                content = handle.read()
        except OSError as exc:
            LOGGER.warning("GTFS catalog read failed: %s (%s)", path, exc)
            return {}, {}, {}

    url_map: dict[str, str] = {}
    base_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        code = (
            row.get("country_code")
            or row.get("location.country_code")
            or row.get("countryCode")
        )
        if not code:
            continue
        code = code.strip().upper()
        if not code:
            continue
        name = (
            row.get("country")
            or row.get("location.country")
            or row.get("country_name")
            or row.get("country_name_en")
            or row.get("country_name_fr")
            or row.get("countryName")
        )
        if name:
            key = _normalize_country_key(name)
            if key:
                name_map.setdefault(key, code)
        for field in (
            "urls.latest",
            "urls.direct_download",
            "download_url",
            "url",
        ):
            url = row.get(field)
            if not url:
                continue
            url = url.strip()
            if not url:
                continue
            url_map.setdefault(url, code)
            base = os.path.basename(urlparse(url).path)
            if base:
                base_map.setdefault(base, code)
    return url_map, base_map, name_map


def _infer_country_code(entry: str, url_map: dict[str, str], base_map: dict[str, str]) -> str | None:
    if entry in url_map:
        return url_map[entry]
    base = os.path.basename(entry)
    if base in base_map:
        return base_map[base]
    return None


def _load_input_paths(
    argv: list[str],
    url_map: dict[str, str] | None = None,
    base_map: dict[str, str] | None = None,
) -> list[tuple[str, str | None]]:
    raw_paths: list[str] = []
    raw_paths.extend(_parse_list_env(os.environ.get(INPUT_FILES_ENV)))

    list_path = os.environ.get(INPUT_FILE_LIST_ENV)
    if list_path and os.path.isfile(list_path):
        with open(list_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    raw_paths.append(line)

    for arg in argv:
        if arg:
            raw_paths.append(arg)

    seen: set[str] = set()
    unique: list[str] = []
    for path in raw_paths:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)

    url_map = url_map or {}
    base_map = base_map or {}
    items: list[tuple[str, str | None]] = []
    for raw in unique:
        entry, explicit = _split_input_spec(raw)
        inferred = _infer_country_code(entry, url_map, base_map)
        items.append((entry, explicit or inferred))
    return items


def _download_file(url: str, tmp_dir: str) -> str | None:
    timeout_raw = os.environ.get(DOWNLOAD_TIMEOUT_ENV)
    try:
        timeout = float(timeout_raw) if timeout_raw else 120.0
    except ValueError:
        timeout = 120.0
    retries_raw = os.environ.get(DOWNLOAD_RETRIES_ENV)
    retries = int(retries_raw) if retries_raw and retries_raw.isdigit() else 2
    chunk_raw = os.environ.get(DOWNLOAD_CHUNK_MB_ENV)
    chunk_mb = int(chunk_raw) if chunk_raw and chunk_raw.isdigit() else 4
    chunk_size = max(1, chunk_mb) * 1024 * 1024

    filename = os.path.basename(url.split("?", 1)[0]) or "download.bin"
    target = os.path.join(tmp_dir, filename)

    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()
            with open(target, "wb") as handle:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        handle.write(chunk)
            return target
        except (requests.RequestException, OSError) as exc:
            try:
                if os.path.exists(target):
                    os.remove(target)
            except OSError:
                pass
            if attempt >= attempts:
                LOGGER.warning("Download failed: %s (%s)", url, exc)
                return None
            LOGGER.warning("Download failed (retry %s/%s): %s (%s)", attempt, attempts - 1, url, exc)
            time.sleep(min(5, attempt))
    return None


def _resolve_local_path(path_or_url: str, tmp_dir: str) -> str | None:
    if os.path.exists(path_or_url):
        return path_or_url
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return _download_file(path_or_url, tmp_dir)
    LOGGER.warning("Input not found: %s", path_or_url)
    return None


def _parse_int_set(value: str | None) -> set[int]:
    if not value:
        return set()
    result: set[int] = set()
    for raw in value.split(","):
        token = raw.strip()
        if not token:
            continue
        if "-" in token:
            start, end = token.split("-", 1)
            start = start.strip()
            end = end.strip()
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


def _allowed_train_route_types() -> set[int]:
    return _parse_int_set(
        os.environ.get(ALLOWED_TRAIN_ROUTE_TYPES_ENV, DEFAULT_ALLOWED_TRAIN_ROUTE_TYPES)
    )


def _allowed_air_route_types() -> set[int]:
    return _parse_int_set(
        os.environ.get(ALLOWED_AIR_ROUTE_TYPES_ENV, DEFAULT_ALLOWED_AIR_ROUTE_TYPES)
    )


def _spark_session() -> SparkSession:
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

    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = SparkSession.builder.appName("obrail-transport-etl").master(master)
    # Force local driver networking to avoid host.docker.internal misconfig.
    builder = builder.config("spark.driver.host", "127.0.0.1")
    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
    python_exec = os.environ.get("PYSPARK_PYTHON") or sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)
    builder = builder.config("spark.pyspark.python", python_exec)
    builder = builder.config("spark.pyspark.driver.python", python_exec)
    builder = builder.config("spark.python.worker.faulthandler.enabled", "true")
    builder = builder.config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")

    driver_mem = os.environ.get(SPARK_DRIVER_MEMORY_ENV)
    executor_mem = os.environ.get(SPARK_EXECUTOR_MEMORY_ENV)
    driver_overhead = os.environ.get(SPARK_DRIVER_MEMORY_OVERHEAD_ENV)
    executor_overhead = os.environ.get(SPARK_EXECUTOR_MEMORY_OVERHEAD_ENV)
    local_dir = os.environ.get(SPARK_LOCAL_DIR_ENV)
    if driver_mem:
        builder = builder.config("spark.driver.memory", driver_mem)
    if executor_mem:
        builder = builder.config("spark.executor.memory", executor_mem)
    if driver_overhead:
        builder = builder.config("spark.driver.memoryOverhead", driver_overhead)
    if executor_overhead:
        builder = builder.config("spark.executor.memoryOverhead", executor_overhead)
    if local_dir:
        builder = builder.config("spark.local.dir", local_dir)

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

    shuffle = os.environ.get(SPARK_SHUFFLE_PARTITIONS_ENV)
    if shuffle and shuffle.isdigit():
        builder = builder.config("spark.sql.shuffle.partitions", shuffle)
    spark = builder.getOrCreate()
    log_level = os.environ.get(SPARK_LOG_LEVEL_ENV, "ERROR")
    try:
        spark.sparkContext.setLogLevel(log_level)
    except Exception:
        pass
    return spark


def _read_csv(
    spark: SparkSession,
    path: str,
    columns: list[str] | None = None,
) -> DataFrame:
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    if columns:
        available = [col for col in columns if col in df.columns]
        if available:
            df = df.select(*available)
    return df


def _extract_gtfs_files(zip_path: str, target_dir: str) -> dict[str, str]:
    needed = {"routes.txt", "trips.txt", "stop_times.txt", "stops.txt"}
    extracted: dict[str, str] = {}
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                base = os.path.basename(info.filename)
                if base in needed:
                    out_path = os.path.join(target_dir, base)
                    with zf.open(info) as src, open(out_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    extracted[base] = out_path
    except zipfile.BadZipFile:
        LOGGER.warning("Invalid ZIP: %s", zip_path)
    return extracted


def _load_gtfs_dir(path: str, tmp_dir: str) -> str | None:
    if os.path.isdir(path):
        return path
    if path.lower().endswith(".zip"):
        extract_dir = tempfile.mkdtemp(prefix="gtfs_", dir=tmp_dir)
        extracted = _extract_gtfs_files(path, extract_dir)
        if not extracted:
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None
        return extract_dir
    return None


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


def _valid_coord_expr(lat_col: F.Column, lon_col: F.Column) -> F.Column:
    return (
        lat_col.isNotNull()
        & lon_col.isNotNull()
        & lat_col.between(-90.0, 90.0)
        & lon_col.between(-180.0, 180.0)
        & (lat_col != 0)
        & (lon_col != 0)
    )


def _station_key_expr(
    name_col: F.Column,
    lat_col: F.Column,
    lon_col: F.Column,
    country_col: F.Column,
) -> F.Column:
    name_norm = F.lower(F.trim(F.coalesce(name_col, F.lit(""))))
    lat_norm = F.round(lat_col, COORD_ROUND_DECIMALS).cast(StringType())
    lon_norm = F.round(lon_col, COORD_ROUND_DECIMALS).cast(StringType())
    country_norm = F.upper(F.trim(F.coalesce(country_col, F.lit(""))))
    return F.concat_ws("||", name_norm, lat_norm, lon_norm, country_norm)


def _train_specificity_expr(text_col: F.Column) -> F.Column:
    lower = F.lower(F.coalesce(text_col, F.lit("")))
    return (
        F.when(lower.contains("tgv"), F.lit("TGV"))
        .when(lower.contains("rer"), F.lit("RER"))
        .when(lower.contains("intercite"), F.lit("Intercite"))
        .when(lower.contains("intercity"), F.lit("Intercity"))
        .when(lower.contains("ter"), F.lit("TER"))
        .when(lower.contains("ice"), F.lit("ICE"))
        .when(lower.contains("thaly"), F.lit("Thalys"))
        .when(lower.contains("eurostar"), F.lit("Eurostar"))
        .when(lower.contains("night") | lower.contains("nuit"), F.lit("Night"))
        .otherwise(F.when(F.trim(text_col) != "", F.trim(text_col)).otherwise(F.lit("Train")))
    )


def _air_specificity_expr(text_col: F.Column) -> F.Column:
    return F.when(F.trim(text_col) != "", F.trim(text_col)).otherwise(F.lit("Avion"))


def _night_hint_expr(text_col: F.Column) -> F.Column:
    lower = F.lower(F.coalesce(text_col, F.lit("")))
    return (
        lower.contains("night")
        | lower.contains("nuit")
        | lower.contains("noct")
        | lower.contains("overnight")
        | lower.contains("sleeper")
        | lower.contains("sleeping")
        | lower.contains("nightjet")
        | lower.contains("couchette")
    )


def _resolve_parent_stops(stops_df: DataFrame) -> DataFrame:
    if "parent_station" not in stops_df.columns:
        return stops_df
    has_country = "stop_country" in stops_df.columns
    parents = (
        stops_df.select(
            F.col("stop_id").alias("parent_id"),
            F.col("stop_name").alias("parent_name"),
            F.col("stop_lat").alias("parent_lat"),
            F.col("stop_lon").alias("parent_lon"),
            F.col("stop_country").alias("parent_country") if has_country else F.lit(None).alias("parent_country"),
        )
    )
    joined = stops_df.join(
        parents,
        F.col("parent_station") == F.col("parent_id"),
        "left",
    )
    joined = joined.withColumn("stop_name", F.coalesce(F.col("parent_name"), F.col("stop_name")))
    joined = joined.withColumn("stop_lat", F.coalesce(F.col("parent_lat"), F.col("stop_lat")))
    joined = joined.withColumn("stop_lon", F.coalesce(F.col("parent_lon"), F.col("stop_lon")))
    if has_country:
        joined = joined.withColumn("stop_country", F.coalesce(F.col("parent_country"), F.col("stop_country")))
    return joined.drop("parent_id", "parent_name", "parent_lat", "parent_lon", "parent_country")


def _extract_trips_from_gtfs_dir(
    spark: SparkSession,
    gtfs_dir: str,
    default_country: str | None = None,
) -> DataFrame | None:
    routes_path = os.path.join(gtfs_dir, "routes.txt")
    trips_path = os.path.join(gtfs_dir, "trips.txt")
    stop_times_path = os.path.join(gtfs_dir, "stop_times.txt")
    stops_path = os.path.join(gtfs_dir, "stops.txt")

    for path in (routes_path, trips_path, stop_times_path, stops_path):
        if not os.path.exists(path):
            LOGGER.warning("Missing GTFS file: %s", path)
            return None

    routes = _read_csv(
        spark,
        routes_path,
        ["route_id", "route_type", "route_short_name", "route_long_name", "route_desc"],
    )
    if "route_desc" not in routes.columns:
        routes = routes.withColumn("route_desc", F.lit(None).cast(StringType()))
    trips = _read_csv(
        spark,
        trips_path,
        ["trip_id", "route_id", "trip_short_name"],
    )
    if "trip_short_name" not in trips.columns:
        trips = trips.withColumn("trip_short_name", F.lit(None).cast(StringType()))
    stop_times = _read_csv(
        spark,
        stop_times_path,
        ["trip_id", "stop_id", "stop_sequence"],
    )
    stops = _read_csv(
        spark,
        stops_path,
        ["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_country", "parent_station"],
    )
    if "stop_country" not in stops.columns:
        stops = stops.withColumn("stop_country", F.lit(None).cast(StringType()))

    if not routes.columns or not trips.columns or not stop_times.columns or not stops.columns:
        return None

    routes = routes.withColumn("route_type_int", F.col("route_type").cast(IntegerType()))
    allowed_train = list(_allowed_train_route_types())
    allowed_air = list(_allowed_air_route_types())
    routes = routes.withColumn(
        "transport_type",
        F.when(F.col("route_type_int").isin(allowed_train), F.lit("train"))
        .when(F.col("route_type_int").isin(allowed_air), F.lit("avion"))
        .otherwise(F.lit(None)),
    )
    routes = routes.filter(F.col("transport_type").isNotNull())

    if routes.limit(1).count() == 0:
        LOGGER.info("GTFS: aucune route train/avion apres filtrage.")
        return None

    routes = routes.withColumn(
        "name_blob",
        F.concat_ws(" ", F.col("route_short_name"), F.col("route_long_name"), F.col("route_desc")),
    )
    routes = routes.withColumn(
        "specificite",
        F.when(F.col("transport_type") == F.lit("train"), _train_specificity_expr(F.col("name_blob")))
        .otherwise(_air_specificity_expr(F.col("name_blob"))),
    )
    routes = routes.withColumn(
        "train_type",
        F.when(F.col("transport_type") == F.lit("train"), F.col("route_type_int")).otherwise(F.lit(None)),
    )
    routes = routes.select("route_id", "transport_type", "specificite", "train_type", "name_blob")

    stop_times = stop_times.withColumn("stop_sequence", F.col("stop_sequence").cast(IntegerType()))
    stop_times = stop_times.filter(F.col("stop_sequence").isNotNull())
    win_first = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").asc())
    win_last = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").desc())
    first_stop = (
        stop_times.withColumn("rn", F.row_number().over(win_first))
        .filter(F.col("rn") == 1)
        .select(F.col("trip_id"), F.col("stop_id").alias("departure_stop_id"))
    )
    last_stop = (
        stop_times.withColumn("rn", F.row_number().over(win_last))
        .filter(F.col("rn") == 1)
        .select(F.col("trip_id"), F.col("stop_id").alias("arrival_stop_id"))
    )

    stops = _resolve_parent_stops(stops)
    default_country = default_country or _default_country_code("gtfs")
    fill_missing = _truthy_env(GTFS_FILL_MISSING_COUNTRY_ENV, True)
    if default_country and fill_missing:
        stop_country = F.col("stop_country")
        stop_country_trim = F.trim(stop_country)
        stops = stops.withColumn(
            "stop_country",
            F.when(
                stop_country.isNull()
                | (stop_country_trim == "")
                | (F.lower(stop_country_trim).isin("nan", "null")),
                F.lit(default_country),
            ).otherwise(stop_country),
        )
    stops = _apply_country_bbox(stops)

    trips = trips.join(routes, "route_id", "inner")
    trips = trips.withColumn(
        "night_blob",
        F.concat_ws(" ", F.col("name_blob"), F.col("trip_short_name")),
    )
    trips = trips.withColumn(
        "is_night",
        F.when(
            (F.col("transport_type") == F.lit("train"))
            & ((F.col("train_type") == F.lit(105)) | _night_hint_expr(F.col("night_blob"))),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    trips = trips.join(first_stop, "trip_id", "inner").join(last_stop, "trip_id", "inner")

    dep = stops.select(
        F.col("stop_id").alias("departure_stop_id"),
        F.col("stop_name").alias("departure_station"),
        F.col("stop_lat").alias("departure_lat"),
        F.col("stop_lon").alias("departure_lon"),
        F.col("stop_country").alias("departure_country"),
    )
    arr = stops.select(
        F.col("stop_id").alias("arrival_stop_id"),
        F.col("stop_name").alias("arrival_station"),
        F.col("stop_lat").alias("arrival_lat"),
        F.col("stop_lon").alias("arrival_lon"),
        F.col("stop_country").alias("arrival_country"),
    )
    trips = trips.join(dep, "departure_stop_id", "left").join(arr, "arrival_stop_id", "left")

    trips = trips.select(
        "transport_type",
        "specificite",
        "train_type",
        "is_night",
        "departure_stop_id",
        "arrival_stop_id",
        "departure_station",
        "departure_lat",
        "departure_lon",
        "departure_country",
        "arrival_station",
        "arrival_lat",
        "arrival_lon",
        "arrival_country",
    )
    return trips


def _find_col(df: DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def _extract_trips_from_flights_csv(
    spark: SparkSession,
    csv_path: str,
    default_country: str | None = None,
) -> DataFrame | None:
    df = _read_csv(spark, csv_path)
    if not df.columns:
        return None

    origin_name = _find_col(df, ["origin_name", "origin", "from", "departure", "dep_name", "origin_station"])
    origin_lat = _find_col(df, ["origin_lat", "dep_lat", "from_lat", "departure_lat", "origin_latitude"])
    origin_lon = _find_col(df, ["origin_lon", "origin_lng", "dep_lon", "from_lon", "departure_lon", "origin_longitude"])
    origin_country = _find_col(df, ["origin_country", "from_country", "departure_country", "origin_iso"])

    dest_name = _find_col(df, ["destination_name", "destination", "to", "arrival", "arr_name", "dest_name"])
    dest_lat = _find_col(df, ["destination_lat", "dest_lat", "to_lat", "arrival_lat", "destination_latitude"])
    dest_lon = _find_col(df, ["destination_lon", "destination_lng", "dest_lon", "to_lon", "arrival_lon", "destination_longitude"])
    dest_country = _find_col(df, ["destination_country", "to_country", "arrival_country", "destination_iso"])

    airline = _find_col(df, ["airline", "carrier", "operator", "company"])

    required = [origin_lat, origin_lon, dest_lat, dest_lon]
    if any(col is None for col in required):
        LOGGER.warning("Flights CSV missing required coord columns: %s", csv_path)
        return None

    df = df.withColumn("transport_type", F.lit("avion"))
    df = df.withColumn("specificite", F.col(airline) if airline else F.lit("Avion"))
    df = df.withColumn("train_type", F.lit(None).cast(IntegerType()))

    df = df.withColumn("departure_station", F.col(origin_name) if origin_name else F.lit("Unknown"))
    df = df.withColumn("departure_lat", F.col(origin_lat))
    df = df.withColumn("departure_lon", F.col(origin_lon))
    df = df.withColumn("departure_country", F.col(origin_country) if origin_country else F.lit(None))

    df = df.withColumn("arrival_station", F.col(dest_name) if dest_name else F.lit("Unknown"))
    df = df.withColumn("arrival_lat", F.col(dest_lat))
    df = df.withColumn("arrival_lon", F.col(dest_lon))
    df = df.withColumn("arrival_country", F.col(dest_country) if dest_country else F.lit(None))

    default_country = default_country or _default_country_code("flights")
    if default_country:
        dep_country = F.col("departure_country")
        arr_country = F.col("arrival_country")
        df = df.withColumn(
            "departure_country",
            F.when(dep_country.isNull() | (F.trim(dep_country) == ""), F.lit(default_country)).otherwise(dep_country),
        )
        df = df.withColumn(
            "arrival_country",
            F.when(arr_country.isNull() | (F.trim(arr_country) == ""), F.lit(default_country)).otherwise(arr_country),
        )

    df = df.withColumn("departure_stop_id", F.lit(None).cast(StringType()))
    df = df.withColumn("arrival_stop_id", F.lit(None).cast(StringType()))

    return df.select(
        "transport_type",
        "specificite",
        "train_type",
        "departure_stop_id",
        "arrival_stop_id",
        "departure_station",
        "departure_lat",
        "departure_lon",
        "departure_country",
        "arrival_station",
        "arrival_lat",
        "arrival_lon",
        "arrival_country",
    )


def _filter_and_distance(df: DataFrame) -> DataFrame:
    min_km_raw = os.environ.get(MIN_DISTANCE_KM_ENV)
    min_km = float(min_km_raw) if min_km_raw else DEFAULT_MIN_DISTANCE_KM
    factor_raw = os.environ.get(DISTANCE_FACTOR_TRAIN_ENV)
    factor_train = float(factor_raw) if factor_raw else DEFAULT_DISTANCE_FACTOR_TRAIN

    df = df.withColumn("departure_lat", F.col("departure_lat").cast(DoubleType()))
    df = df.withColumn("departure_lon", F.col("departure_lon").cast(DoubleType()))
    df = df.withColumn("arrival_lat", F.col("arrival_lat").cast(DoubleType()))
    df = df.withColumn("arrival_lon", F.col("arrival_lon").cast(DoubleType()))

    valid_dep = _valid_coord_expr(F.col("departure_lat"), F.col("departure_lon"))
    valid_arr = _valid_coord_expr(F.col("arrival_lat"), F.col("arrival_lon"))
    df = df.filter(valid_dep & valid_arr)

    dist_raw = _haversine_km_expr(
        F.col("departure_lat"),
        F.col("departure_lon"),
        F.col("arrival_lat"),
        F.col("arrival_lon"),
    )
    df = df.withColumn("distance_km_raw", dist_raw)
    df = df.filter(F.col("distance_km_raw").isNotNull() & (F.col("distance_km_raw") > 0))
    df = df.filter(F.col("distance_km_raw") >= F.lit(min_km))

    df = df.withColumn(
        "distance_km",
        F.when(F.col("transport_type") == F.lit("train"), F.col("distance_km_raw") * F.lit(factor_train))
        .otherwise(F.col("distance_km_raw")),
    )
    return df.drop("distance_km_raw")


def _build_station_dim(df: DataFrame) -> DataFrame:
    dep = df.select(
        F.col("departure_stop_id").alias("stop_id"),
        F.col("departure_station").alias("station_name"),
        F.col("departure_lat").alias("latitude"),
        F.col("departure_lon").alias("longitude"),
        F.col("departure_country").alias("pays"),
    )
    arr = df.select(
        F.col("arrival_stop_id").alias("stop_id"),
        F.col("arrival_station").alias("station_name"),
        F.col("arrival_lat").alias("latitude"),
        F.col("arrival_lon").alias("longitude"),
        F.col("arrival_country").alias("pays"),
    )
    stations = dep.unionByName(arr)
    stations = stations.withColumn("latitude", F.round(F.col("latitude"), COORD_ROUND_DECIMALS))
    stations = stations.withColumn("longitude", F.round(F.col("longitude"), COORD_ROUND_DECIMALS))
    stations = stations.withColumn("station_name", F.trim(F.col("station_name")))
    stations = stations.withColumn("pays", F.upper(F.trim(F.col("pays"))))
    stations = stations.withColumn(
        "stop_id",
        F.when(F.trim(F.col("stop_id")) == "", F.lit(None)).otherwise(F.col("stop_id")),
    )
    stations = stations.withColumn(
        "_has_stop_id",
        F.when(F.col("stop_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
    win = Window.partitionBy("station_name", "latitude", "longitude", "pays").orderBy(
        F.col("_has_stop_id").desc(), F.col("stop_id").asc()
    )
    stations = stations.withColumn("rn", F.row_number().over(win)).filter(F.col("rn") == 1)
    stations = stations.drop("rn", "_has_stop_id")

    win = Window.partitionBy(F.lit(1)).orderBy("station_name", "latitude", "longitude", "pays")
    stations = stations.withColumn("station_id", F.row_number().over(win))
    return stations.select("station_id", "stop_id", "station_name", "latitude", "longitude", "pays")


def _build_vehicle_dim(df: DataFrame) -> DataFrame:
    vehicles = df.select(
        F.col("transport_type").alias("type_transport"),
        F.col("specificite"),
        F.col("train_type"),
    ).dropDuplicates()
    win = Window.partitionBy(F.lit(1)).orderBy("type_transport", "specificite", "train_type")
    vehicles = vehicles.withColumn("vehicule_id", F.row_number().over(win))
    return vehicles.select("vehicule_id", "type_transport", "specificite", "train_type")


def _build_trajet_fact(df: DataFrame, stations: DataFrame, vehicles: DataFrame) -> DataFrame:
    df = df.withColumn(
        "dep_key",
        _station_key_expr(
            F.col("departure_station"),
            F.col("departure_lat"),
            F.col("departure_lon"),
            F.col("departure_country"),
        ),
    )
    df = df.withColumn(
        "arr_key",
        _station_key_expr(
            F.col("arrival_station"),
            F.col("arrival_lat"),
            F.col("arrival_lon"),
            F.col("arrival_country"),
        ),
    )

    stations = stations.withColumn(
        "station_key",
        _station_key_expr(
            F.col("station_name"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("pays"),
        ),
    )

    dep_join = stations.select(
        F.col("station_key").alias("dep_key"),
        F.col("station_id").alias("departure_station_id"),
    )
    arr_join = stations.select(
        F.col("station_key").alias("arr_key"),
        F.col("station_id").alias("arrival_station_id"),
    )

    df = df.join(dep_join, "dep_key", "left").join(arr_join, "arr_key", "left")

    vehicles_join = vehicles.withColumnRenamed("train_type", "veh_train_type")
    t = df.alias("t")
    v = vehicles_join.alias("v")
    df = t.join(
        v,
        (F.col("t.transport_type") == F.col("v.type_transport"))
        & (F.col("t.specificite").eqNullSafe(F.col("v.specificite")))
        & (F.col("t.train_type").eqNullSafe(F.col("v.veh_train_type"))),
        "left",
    )

    if "is_night" not in df.columns:
        df = df.withColumn(
            "is_night",
            F.when((F.col("transport_type") == F.lit("train")) & (F.col("train_type") == F.lit(105)), F.lit(True))
            .otherwise(F.lit(False)),
        )

    co2_train_raw = os.environ.get(CO2_PER_KM_TRAIN_ENV)
    co2_air_raw = os.environ.get(CO2_PER_KM_AIR_ENV)
    co2_train = float(co2_train_raw) if co2_train_raw else DEFAULT_CO2_PER_KM_TRAIN
    co2_air = float(co2_air_raw) if co2_air_raw else DEFAULT_CO2_PER_KM_AIR

    df = df.withColumn(
        "co2_kg",
        F.when(F.col("transport_type") == F.lit("train"), F.col("distance_km") * F.lit(co2_train))
        .otherwise(F.col("distance_km") * F.lit(co2_air)),
    )

    df = df.withColumn("pair_a", F.least(F.col("departure_station_id"), F.col("arrival_station_id")))
    df = df.withColumn("pair_b", F.greatest(F.col("departure_station_id"), F.col("arrival_station_id")))
    df = df.withColumn("departure_station_id", F.col("pair_a"))
    df = df.withColumn("arrival_station_id", F.col("pair_b"))

    df = df.dropDuplicates(["vehicule_id", "pair_a", "pair_b"])

    win = Window.partitionBy(F.lit(1)).orderBy("vehicule_id", "pair_a", "pair_b")
    df = df.withColumn("trajet_id", F.row_number().over(win))

    return df.select(
        "trajet_id",
        "vehicule_id",
        "is_night",
        "departure_station_id",
        "arrival_station_id",
        "distance_km",
        "co2_kg",
    )


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "obrail"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "143123!"),
    )


def _jdbc_url() -> str:
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "obrail")
    return f"jdbc:postgresql://{host}:{port}/{dbname}"


def _write_df_jdbc(df: DataFrame, table_name: str) -> None:
    props = {
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "143123!"),
        "driver": "org.postgresql.Driver",
    }
    partitions_raw = os.environ.get(JDBC_WRITE_PARTITIONS_ENV)
    if partitions_raw and partitions_raw.isdigit():
        target = int(partitions_raw)
        if target > 0:
            df = df.coalesce(target)

    batch_raw = os.environ.get(JDBC_BATCH_SIZE_ENV)
    batch_size = int(batch_raw) if batch_raw and batch_raw.isdigit() else None

    def _do_write(frame: DataFrame, batch: int | None = None) -> None:
        writer = frame.write.mode("append")
        if batch and batch > 0:
            writer = writer.option("batchsize", str(batch))
        writer.jdbc(_jdbc_url(), table_name, properties=props)

    try:
        _do_write(df, batch_size)
    except Exception as exc:
        retry = _truthy_env(JDBC_RETRY_ON_ERROR_ENV, True)
        if not retry:
            raise
        fallback_partitions_raw = os.environ.get(JDBC_FALLBACK_PARTITIONS_ENV)
        fallback_partitions = (
            int(fallback_partitions_raw)
            if fallback_partitions_raw and fallback_partitions_raw.isdigit()
            else 4
        )
        fallback_batch_raw = os.environ.get(JDBC_FALLBACK_BATCH_SIZE_ENV)
        fallback_batch = (
            int(fallback_batch_raw)
            if fallback_batch_raw and fallback_batch_raw.isdigit()
            else 100
        )
        LOGGER.warning(
            "JDBC write failed for %s (%s). Retry with partitions=%s batchsize=%s.",
            table_name,
            exc,
            fallback_partitions,
            fallback_batch,
        )
        frame = df.repartition(fallback_partitions) if fallback_partitions > 0 else df
        _do_write(frame, fallback_batch)


def _ensure_schema(schema_path: str, schema_name: str, truncate: bool = True) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            if os.path.exists(schema_path):
                with open(schema_path, "r", encoding="utf-8") as handle:
                    content = handle.read()
                    if schema_name != DEFAULT_SCHEMA:
                        content = content.replace(DEFAULT_SCHEMA, schema_name)
                    cur.execute(content)
            if truncate:
                cur.execute(
                    f"TRUNCATE {schema_name}.trajet, {schema_name}.vehicule, {schema_name}.station CASCADE"
                )
        conn.commit()


def _get_gtfs_sources_by_country_code(
    catalog_url: str,
    country_codes: list[str],
    max_sources: int | None = None,
    per_country: int | None = None,
) -> list[tuple[str, str]]:
    import pandas as pd

    try:
        df = pd.read_csv(catalog_url)
    except Exception as exc:
        LOGGER.warning(f"Impossible de lire le catalogue GTFS: {catalog_url} ({exc})")
        return []

    code_col = None
    for candidate in ("location.country_code", "country_code", "countryCode"):
        if candidate in df.columns:
            code_col = candidate
            break
    if not code_col:
        LOGGER.warning("Catalogue GTFS sans colonne country_code.")
        return []

    def _is_probable_gtfs_url(value: str) -> bool:
        if not value:
            return False
        lower = value.strip().lower()
        if not lower:
            return False
        if any(lower.endswith(ext) for ext in (".zip", ".gtfs", ".csv")):
            return True
        return "gtfs" in lower

    def _pick_url(row) -> str | None:
        for candidate in ("urls.latest", "urls.direct_download", "download_url", "url"):
            if candidate in df.columns:
                value = row.get(candidate)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    if not any(c in df.columns for c in ("urls.latest", "urls.direct_download", "download_url", "url")):
        LOGGER.warning("Catalogue GTFS sans colonne d'URL.")
        return []

    def _url_downloadable(url: str, timeout: float) -> bool:
        try:
            resp = requests.head(url, allow_redirects=True, timeout=timeout)
            if resp.status_code < 400:
                return True
        except requests.RequestException:
            pass
        try:
            resp = requests.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    break
            return True
        except requests.RequestException:
            return False

    validate_urls = _truthy_env(GTFS_VALIDATE_URLS_ENV, True)
    timeout_raw = os.environ.get(GTFS_VALIDATE_TIMEOUT_ENV)
    try:
        timeout = float(timeout_raw) if timeout_raw else 20.0
    except ValueError:
        timeout = 20.0
    max_candidates_raw = os.environ.get(GTFS_VALIDATE_MAX_CANDIDATES_ENV)
    max_candidates = int(max_candidates_raw) if max_candidates_raw and max_candidates_raw.isdigit() else 50

    codes = df[code_col].astype(str).str.upper()
    allowed = {c.upper() for c in country_codes}
    mask = codes.isin(list(allowed))
    entries: list[tuple[str, str]] = []
    per_counts: dict[str, int] = {}
    tested_counts: dict[str, int] = {}
    failed_validations = 0
    skipped = 0
    for idx in df.index[mask]:
        row = df.loc[idx]
        url = _pick_url(row)
        code = codes.loc[idx]
        if not isinstance(url, str) or not url.strip():
            skipped += 1
            continue
        if not isinstance(code, str) or not code.strip():
            skipped += 1
            continue
        if not _is_probable_gtfs_url(url):
            skipped += 1
            continue
        code = code.strip().upper()
        if per_country is not None:
            current = per_counts.get(code, 0)
            if current >= per_country:
                continue
            tested = tested_counts.get(code, 0)
            if tested >= max_candidates:
                continue
        if validate_urls:
            tested_counts[code] = tested_counts.get(code, 0) + 1
            if not _url_downloadable(url.strip(), timeout):
                failed_validations += 1
                continue
        entries.append((url.strip(), code))
        if per_country is not None:
            per_counts[code] = per_counts.get(code, 0) + 1
            if per_counts[code] >= per_country and len(per_counts) == len(allowed):
                if all(per_counts.get(c, 0) >= per_country for c in allowed):
                    break
        if per_country is None and max_sources is not None and len(entries) >= max_sources:
            break

    if skipped:
        LOGGER.info("GTFS catalog: %s URLs ignorees (non GTFS ou vides).", skipped)
    if validate_urls:
        if failed_validations:
            LOGGER.info("GTFS catalog: %s URLs invalides ignorees.", failed_validations)
        if per_country:
            summary = ", ".join(f"{code}:{per_counts.get(code,0)}" for code in sorted(allowed))
            LOGGER.info("GTFS catalog: sélection par pays -> %s", summary)
    return entries


def run_stream_etl(argv: list[str] | None = None) -> None:
    _setup_logger()
    argv = argv or []

    tmp_root = tempfile.mkdtemp(prefix="transport_etl_")
    spark = _spark_session()
    try:
        url_map, base_map, name_map = _load_gtfs_catalog_country_map()
        ignorelist_enabled = _truthy_env(GTFS_IGNORELIST_ENABLED_ENV, True)
        ignorelist_reset = _truthy_env(GTFS_IGNORELIST_RESET_ENV, False)
        ignorelist_path = os.environ.get(GTFS_IGNORELIST_PATH_ENV) or _default_ignorelist_path()
        ignorelist: set[str] = set()
        if ignorelist_enabled and not ignorelist_reset:
            ignorelist = _load_ignorelist(ignorelist_path)
            if ignorelist:
                LOGGER.info("GTFS ignorelist active (%s URLs).", len(ignorelist))
        snapshot_dir = os.environ.get(ETL_SNAPSHOT_DIR_ENV) or _default_snapshot_dir()
        snapshot_load_raw = os.environ.get(ETL_SNAPSHOT_LOAD_ENV)
        snapshot_ids = _parse_list_env(snapshot_load_raw)
        snapshot_mode = len(snapshot_ids) > 0

        merged: dict[str, str | None] = {}
        if not snapshot_mode:
            inputs = _load_input_paths(argv, url_map, base_map)
            # Ajout des GTFS dynamiques par country_code depuis un catalogue CSV
            catalog_url = (
                os.environ.get("GTFS_CATALOG_URL")
                or os.environ.get("MOBILITY_DATABASE_CATALOG_PATH")
                or os.environ.get("MOBILITY_DATABASE_CATALOG_URL")
                or GTFS_CATALOG_URL
            )
            country_codes_env = os.environ.get("GTFS_COUNTRY_CODES")
            if country_codes_env:
                country_codes = [c.strip().upper() for c in country_codes_env.split(",") if c.strip()]
            else:
                country_codes = sorted(GTFS_COUNTRY_CODES)
            max_sources_env = os.environ.get("GTFS_MAX_SOURCES")
            per_country_env = os.environ.get("GTFS_MAX_SOURCES_PER_COUNTRY")
            per_country = int(per_country_env) if per_country_env and per_country_env.isdigit() else None
            if per_country is None:
                per_country = int(max_sources_env) if max_sources_env and max_sources_env.isdigit() else GTFS_MAX_SOURCES
            if catalog_url and country_codes:
                LOGGER.info(
                    "GTFS catalog countries: %s (max per country=%s)",
                    ",".join(country_codes),
                    per_country,
                )
            if catalog_url and country_codes:
                dynamic_gtfs_urls = _get_gtfs_sources_by_country_code(
                    catalog_url,
                    country_codes,
                    max_sources=None,
                    per_country=per_country,
                )
                inputs.extend(dynamic_gtfs_urls)
            # Ajout des GTFS_SOURCES en parall?le (optionnel)
            include_static = _truthy_env(GTFS_INCLUDE_STATIC_SOURCES_ENV, True)
            if include_static:
                static_codes: set[str] = set()
                for src in GTFS_SOURCES:
                    url = src.get("url")
                    if not url:
                        continue
                    code = _country_name_to_code(src.get("country"), name_map)
                    if code:
                        static_codes.add(code)
                    inputs.append((url, code))
                if static_codes:
                    LOGGER.info("GTFS sources statiques: %s", ",".join(sorted(static_codes)))
            else:
                LOGGER.info("GTFS sources statiques: desactivees")

            skipped_ignore = 0
            for raw in inputs:
                if isinstance(raw, tuple):
                    entry, code = raw
                else:
                    entry, code = _split_input_spec(str(raw))
                if not entry:
                    continue
                if ignorelist_enabled and entry in ignorelist:
                    skipped_ignore += 1
                    continue
                if not code:
                    code = _infer_country_code(entry, url_map, base_map)
                if entry in merged:
                    if merged[entry] is None and code:
                        merged[entry] = code
                    continue
                merged[entry] = code
            if skipped_ignore:
                LOGGER.info("GTFS ignorelist: %s URLs ignores.", skipped_ignore)

            if not merged:
                LOGGER.warning("No inputs provided. Set INPUT_FILES or INPUT_FILE_LIST.")
                return
        else:
            LOGGER.info("Snapshot load mode: %s", ",".join(snapshot_ids))

        trips_frames: list[DataFrame] = []
        gtfs_entries: set[str] = set()
        gtfs_zero: set[str] = set()
        if snapshot_mode:
            for sid in snapshot_ids:
                path = sid
                if not os.path.exists(path):
                    path = os.path.join(snapshot_dir, sid)
                trips_path = os.path.join(path, "trips.parquet")
                if not os.path.exists(trips_path):
                    LOGGER.warning("Snapshot introuvable: %s", trips_path)
                    continue
                trips_frames.append(spark.read.parquet(trips_path))
        else:
            for entry, country_code in merged.items():
                local_path = _resolve_local_path(entry, tmp_root)
                if not local_path:
                    continue

                gtfs_dir = _load_gtfs_dir(local_path, tmp_root)
                if gtfs_dir:
                    gtfs_entries.add(entry)
                    gtfs_df = _extract_trips_from_gtfs_dir(spark, gtfs_dir, default_country=country_code)
                    if gtfs_df is not None:
                        gtfs_df = gtfs_df.withColumn("_source", F.lit(entry))
                        gtfs_df = gtfs_df.withColumn("_country", F.lit(country_code))
                        trips_frames.append(gtfs_df)
                    else:
                        LOGGER.info("GTFS feed ignore (aucun trip extrait): %s", entry)
                        gtfs_zero.add(entry)
                    continue

                if local_path.lower().endswith(".csv"):
                    flights_df = _extract_trips_from_flights_csv(spark, local_path, default_country=country_code)
                    if flights_df is not None:
                        flights_df = flights_df.withColumn("_source", F.lit(entry))
                        flights_df = flights_df.withColumn("_country", F.lit(country_code))
                        trips_frames.append(flights_df)
                    else:
                        LOGGER.info("Flights feed ignore (aucun trip extrait): %s", entry)
                    continue

                LOGGER.warning("Unsupported input type: %s", local_path)

        if not trips_frames:
            LOGGER.warning("No trips extracted.")
            return

        trips_df = trips_frames[0]
        for frame in trips_frames[1:]:
            trips_df = trips_df.unionByName(frame, allowMissingColumns=True)

        pre_counts: dict[tuple[str, str | None], int] = {}
        if "_source" in trips_df.columns:
            for row in trips_df.groupBy("_source", "_country").count().collect():
                key = (row["_source"], row["_country"])
                pre_counts[key] = int(row["count"])

        if not snapshot_mode:
            trips_df = _filter_and_distance(trips_df)
            if trips_df.limit(1).count() == 0:
                LOGGER.warning("No trips left after filtering.")
                return
        if pre_counts and "_source" in trips_df.columns:
            post_counts: dict[tuple[str, str | None], int] = {}
            for row in trips_df.groupBy("_source", "_country").count().collect():
                key = (row["_source"], row["_country"])
                post_counts[key] = int(row["count"])
            for (source, country), pre_count in pre_counts.items():
                post_count = post_counts.get((source, country), 0)
                LOGGER.info(
                    "Feed stats: %s country=%s extracted=%s after_filter=%s",
                    source,
                    country or "-",
                    pre_count,
                    post_count,
                )
                if source in gtfs_entries and post_count == 0:
                    gtfs_zero.add(source)

        snapshot_write = _truthy_env(ETL_SNAPSHOT_WRITE_ENV, False)
        if snapshot_write:
            base_id = os.environ.get(ETL_SNAPSHOT_ID_ENV) or _snapshot_id()
            snapshot_id = _next_snapshot_id(base_id, snapshot_dir)
            out_dir = os.path.join(snapshot_dir, snapshot_id)
            trips_df.write.mode("overwrite").parquet(os.path.join(out_dir, "trips.parquet"))
            LOGGER.info("Snapshot ecrit: %s", out_dir)

        if "_source" in trips_df.columns:
            trips_df = trips_df.drop("_source", "_country")

        stations_df = _build_station_dim(trips_df)
        vehicles_df = _build_vehicle_dim(trips_df)
        trajet_df = _build_trajet_fact(trips_df, stations_df, vehicles_df)

        schema_name = os.environ.get(PGSCHEMA_ENV, DEFAULT_SCHEMA)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        schema_path = os.path.join(project_root, "data", "scripts", "mart", "schema_transport.sql")

        truncate = _truthy_env(ETL_TRUNCATE_ENV, True)
        _ensure_schema(schema_path, schema_name, truncate=truncate)

        _write_df_jdbc(stations_df, f"{schema_name}.station")
        _write_df_jdbc(vehicles_df, f"{schema_name}.vehicule")
        _write_df_jdbc(trajet_df, f"{schema_name}.trajet")

        LOGGER.info("Transport ETL complete.")
        if ignorelist_enabled and gtfs_zero:
            updated = set(ignorelist)
            updated.update(gtfs_zero)
            _save_ignorelist(ignorelist_path, updated)
            LOGGER.info("GTFS ignorelist mis a jour (%s URLs).", len(updated))
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    import sys

    run_stream_etl(sys.argv[1:])
