CREATE SCHEMA IF NOT EXISTS obrail;

CREATE TABLE IF NOT EXISTS obrail.dim_country (
    country_key INTEGER PRIMARY KEY,
    country_code VARCHAR(4) NOT NULL,
    country_name_en VARCHAR(128),
    country_name_fr VARCHAR(128),
    iso3_code VARCHAR(4),
    eu_member CHAR(1),
    efta_member CHAR(1),
    candidate_member CHAR(1)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_country_code ON obrail.dim_country (country_code);

CREATE TABLE IF NOT EXISTS obrail.dim_operator (
    operator_key INTEGER PRIMARY KEY,
    operator_id VARCHAR(64) NOT NULL,
    operator_name VARCHAR(256),
    operator_country VARCHAR(64),
    is_night_operator BOOLEAN DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_operator_id ON obrail.dim_operator (operator_id);

ALTER TABLE IF EXISTS obrail.dim_operator
    ALTER COLUMN operator_country TYPE VARCHAR(64);

CREATE TABLE IF NOT EXISTS obrail.dim_station (
    station_key INTEGER PRIMARY KEY,
    stop_id VARCHAR(128) NOT NULL,
    station_name VARCHAR(256),
    station_lat DOUBLE PRECISION,
    station_lon DOUBLE PRECISION,
    country_code VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS ix_dim_station_stop_id ON obrail.dim_station (stop_id);
CREATE INDEX IF NOT EXISTS ix_dim_station_country ON obrail.dim_station (country_code);

ALTER TABLE IF EXISTS obrail.dim_station
    ALTER COLUMN country_code TYPE VARCHAR(64);

CREATE TABLE IF NOT EXISTS obrail.dim_route (
    route_key INTEGER PRIMARY KEY,
    route_id VARCHAR(128) NOT NULL,
    operator_id VARCHAR(64) NOT NULL,
    country_code VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS ix_dim_route_operator ON obrail.dim_route (operator_id);
CREATE INDEX IF NOT EXISTS ix_dim_route_country ON obrail.dim_route (country_code);

ALTER TABLE IF EXISTS obrail.dim_route
    ALTER COLUMN country_code TYPE VARCHAR(64);

CREATE TABLE IF NOT EXISTS obrail.dim_time (
    time_key INTEGER PRIMARY KEY,
    time_value TIME NOT NULL,
    hour SMALLINT,
    minute SMALLINT,
    second SMALLINT,
    is_night BOOLEAN
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_time_value ON obrail.dim_time (time_value);

CREATE TABLE IF NOT EXISTS obrail.dim_date (
    date_key INTEGER PRIMARY KEY,
    date_value DATE NOT NULL,
    year SMALLINT,
    month SMALLINT,
    day SMALLINT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_date_value ON obrail.dim_date (date_value);

CREATE TABLE IF NOT EXISTS obrail.fact_trip_segment (
    fact_trip_key BIGINT PRIMARY KEY,
    country_key INTEGER REFERENCES obrail.dim_country(country_key),
    operator_key INTEGER REFERENCES obrail.dim_operator(operator_key),
    route_key INTEGER REFERENCES obrail.dim_route(route_key),
    departure_station_key INTEGER REFERENCES obrail.dim_station(station_key),
    arrival_station_key INTEGER REFERENCES obrail.dim_station(station_key),
    departure_time_key INTEGER REFERENCES obrail.dim_time(time_key),
    arrival_time_key INTEGER REFERENCES obrail.dim_time(time_key),
    date_key INTEGER REFERENCES obrail.dim_date(date_key),
    trip_business_id VARCHAR(128),
    is_night BOOLEAN,
    is_cross_border BOOLEAN
);

CREATE TABLE IF NOT EXISTS obrail.trip_stop (
    trip_stop_key BIGINT PRIMARY KEY,
    country_code VARCHAR(64),
    operator_id VARCHAR(64),
    trip_id VARCHAR(128),
    stop_sequence INTEGER,
    stop_id VARCHAR(128),
    stop_name VARCHAR(256),
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    arrival_time TIME,
    departure_time TIME,
    date_value DATE
);

CREATE INDEX IF NOT EXISTS ix_trip_stop_trip ON obrail.trip_stop (trip_id);
CREATE INDEX IF NOT EXISTS ix_trip_stop_operator ON obrail.trip_stop (operator_id);
CREATE INDEX IF NOT EXISTS ix_trip_stop_country ON obrail.trip_stop (country_code);
CREATE INDEX IF NOT EXISTS ix_trip_stop_sequence ON obrail.trip_stop (trip_id, stop_sequence);

CREATE INDEX IF NOT EXISTS ix_fact_trip_country ON obrail.fact_trip_segment (country_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_operator ON obrail.fact_trip_segment (operator_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_route ON obrail.fact_trip_segment (route_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_departure_station ON obrail.fact_trip_segment (departure_station_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_arrival_station ON obrail.fact_trip_segment (arrival_station_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip_date ON obrail.fact_trip_segment (date_key);
