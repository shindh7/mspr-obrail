CREATE SCHEMA IF NOT EXISTS obrail_transport;

CREATE TABLE IF NOT EXISTS obrail_transport.vehicule (
    vehicule_id INTEGER PRIMARY KEY,
    type_transport VARCHAR(16) NOT NULL,
    specificite VARCHAR(256),
    train_type INTEGER
);

CREATE TABLE IF NOT EXISTS obrail_transport.station (
    station_id INTEGER PRIMARY KEY,
    stop_id VARCHAR(128),
    station_name VARCHAR(256),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    pays VARCHAR(8)
);

ALTER TABLE IF EXISTS obrail_transport.station
    ADD COLUMN IF NOT EXISTS stop_id VARCHAR(128);

CREATE TABLE IF NOT EXISTS obrail_transport.trajet (
    trajet_id BIGINT PRIMARY KEY,
    vehicule_id INTEGER REFERENCES obrail_transport.vehicule(vehicule_id),
    is_night BOOLEAN,
    departure_station_id INTEGER REFERENCES obrail_transport.station(station_id),
    arrival_station_id INTEGER REFERENCES obrail_transport.station(station_id),
    distance_km DOUBLE PRECISION,
    co2_kg DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS ix_trajet_vehicule ON obrail_transport.trajet (vehicule_id);
CREATE INDEX IF NOT EXISTS ix_trajet_departure ON obrail_transport.trajet (departure_station_id);
CREATE INDEX IF NOT EXISTS ix_trajet_arrival ON obrail_transport.trajet (arrival_station_id);
