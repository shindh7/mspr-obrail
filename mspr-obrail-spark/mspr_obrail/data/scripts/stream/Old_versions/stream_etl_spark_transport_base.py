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

import logging
import os
import shutil
import tempfile
import zipfile

import psycopg2
import requests
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

LOGGER = logging.getLogger("stream_etl_spark_transport")

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
SPARK_SHUFFLE_PARTITIONS_ENV = "SPARK_SHUFFLE_PARTITIONS"

DEFAULT_ALLOWED_TRAIN_ROUTE_TYPES = "2,100-117"
DEFAULT_ALLOWED_AIR_ROUTE_TYPES = "1100-1107"
DEFAULT_MIN_DISTANCE_KM = 100
DEFAULT_DISTANCE_FACTOR_TRAIN = 1.3
DEFAULT_CO2_PER_KM_TRAIN = 0.0
DEFAULT_CO2_PER_KM_AIR = 0.0
DEFAULT_SCHEMA = "obrail_transport"
COORD_ROUND_DECIMALS = 4
EARTH_RADIUS_KM = 6371.0


def _setup_logger() -> None:
    if LOGGER.handlers:
        return
    LOGGER.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


def _parse_list_env(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _load_input_paths(argv: list[str]) -> list[str]:
    paths: list[str] = []
    paths.extend(_parse_list_env(os.environ.get(INPUT_FILES_ENV)))

    list_path = os.environ.get(INPUT_FILE_LIST_ENV)
    if list_path and os.path.isfile(list_path):
        with open(list_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    paths.append(line)

    for arg in argv:
        if arg:
            paths.append(arg)

    seen: set[str] = set()
    unique: list[str] = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def _download_file(url: str, tmp_dir: str) -> str | None:
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Download failed: %s (%s)", url, exc)
        return None

    filename = os.path.basename(url.split("?", 1)[0]) or "download.bin"
    target = os.path.join(tmp_dir, filename)
    try:
        with open(target, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    except OSError as exc:
        LOGGER.warning("Failed to write %s (%s)", target, exc)
        return None
    return target


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
    builder = SparkSession.builder.appName("obrail-transport-etl")
    shuffle = os.environ.get(SPARK_SHUFFLE_PARTITIONS_ENV)
    if shuffle and shuffle.isdigit():
        builder = builder.config("spark.sql.shuffle.partitions", shuffle)
    return builder.getOrCreate()


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


def _resolve_parent_stops(stops_df: DataFrame) -> DataFrame:
    if "parent_station" not in stops_df.columns:
        return stops_df
    parents = (
        stops_df.select(
            F.col("stop_id").alias("parent_id"),
            F.col("stop_name").alias("parent_name"),
            F.col("stop_lat").alias("parent_lat"),
            F.col("stop_lon").alias("parent_lon"),
            F.col("stop_country").alias("parent_country"),
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
    joined = joined.withColumn("stop_country", F.coalesce(F.col("parent_country"), F.col("stop_country")))
    return joined.drop("parent_id", "parent_name", "parent_lat", "parent_lon", "parent_country")


def _extract_trips_from_gtfs_dir(spark: SparkSession, gtfs_dir: str) -> DataFrame | None:
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
    trips = _read_csv(
        spark,
        trips_path,
        ["trip_id", "route_id"],
    )
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

    if routes.rdd.isEmpty():
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
    routes = routes.select("route_id", "transport_type", "specificite", "train_type")

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

    trips = trips.join(routes, "route_id", "inner")
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


def _extract_trips_from_flights_csv(spark: SparkSession, csv_path: str) -> DataFrame | None:
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

    return df.select(
        "transport_type",
        "specificite",
        "train_type",
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
        F.col("departure_station").alias("station_name"),
        F.col("departure_lat").alias("latitude"),
        F.col("departure_lon").alias("longitude"),
        F.col("departure_country").alias("pays"),
    )
    arr = df.select(
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
    stations = stations.dropDuplicates(["station_name", "latitude", "longitude", "pays"])

    win = Window.orderBy("station_name", "latitude", "longitude", "pays")
    stations = stations.withColumn("station_id", F.row_number().over(win))
    return stations.select("station_id", "station_name", "latitude", "longitude", "pays")


def _build_vehicle_dim(df: DataFrame) -> DataFrame:
    vehicles = df.select(
        F.col("transport_type").alias("type_transport"),
        F.col("specificite"),
        F.col("train_type"),
    ).dropDuplicates()
    win = Window.orderBy("type_transport", "specificite", "train_type")
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

    df = df.join(
        vehicles,
        (df["transport_type"] == vehicles["type_transport"])
        & (df["specificite"].eqNullSafe(vehicles["specificite"]))
        & (df["train_type"].eqNullSafe(vehicles["train_type"])),
        "left",
    )

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

    win = Window.orderBy("vehicule_id", "pair_a", "pair_b")
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
    df.write.mode("append").jdbc(_jdbc_url(), table_name, properties=props)


def _ensure_schema(schema_path: str, schema_name: str) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            if os.path.exists(schema_path):
                with open(schema_path, "r", encoding="utf-8") as handle:
                    content = handle.read()
                    if schema_name != DEFAULT_SCHEMA:
                        content = content.replace(DEFAULT_SCHEMA, schema_name)
                    cur.execute(content)
            cur.execute(
                f"TRUNCATE {schema_name}.trajet, {schema_name}.vehicule, {schema_name}.station CASCADE"
            )
        conn.commit()


def run_stream_etl(argv: list[str] | None = None) -> None:
    _setup_logger()
    argv = argv or []

    tmp_root = tempfile.mkdtemp(prefix="transport_etl_")
    spark = _spark_session()
    try:
        inputs = _load_input_paths(argv)
        if not inputs:
            LOGGER.warning("No inputs provided. Set INPUT_FILES or INPUT_FILE_LIST.")
            return

        trips_frames: list[DataFrame] = []
        for entry in inputs:
            local_path = _resolve_local_path(entry, tmp_root)
            if not local_path:
                continue

            gtfs_dir = _load_gtfs_dir(local_path, tmp_root)
            if gtfs_dir:
                gtfs_df = _extract_trips_from_gtfs_dir(spark, gtfs_dir)
                if gtfs_df is not None:
                    trips_frames.append(gtfs_df)
                continue

            if local_path.lower().endswith(".csv"):
                flights_df = _extract_trips_from_flights_csv(spark, local_path)
                if flights_df is not None:
                    trips_frames.append(flights_df)
                continue

            LOGGER.warning("Unsupported input type: %s", local_path)

        if not trips_frames:
            LOGGER.warning("No trips extracted.")
            return

        trips_df = trips_frames[0]
        for frame in trips_frames[1:]:
            trips_df = trips_df.unionByName(frame, allowMissingColumns=True)

        trips_df = _filter_and_distance(trips_df)
        if trips_df.rdd.isEmpty():
            LOGGER.warning("No trips left after filtering.")
            return

        stations_df = _build_station_dim(trips_df)
        vehicles_df = _build_vehicle_dim(trips_df)
        trajet_df = _build_trajet_fact(trips_df, stations_df, vehicles_df)

        schema_name = os.environ.get(PGSCHEMA_ENV, DEFAULT_SCHEMA)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        schema_path = os.path.join(project_root, "data", "scripts", "mart", "schema_transport.sql")

        _ensure_schema(schema_path, schema_name)

        _write_df_jdbc(stations_df, f"{schema_name}.station")
        _write_df_jdbc(vehicles_df, f"{schema_name}.vehicule")
        _write_df_jdbc(trajet_df, f"{schema_name}.trajet")

        LOGGER.info("Transport ETL complete.")
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    import sys

    run_stream_etl(sys.argv[1:])
