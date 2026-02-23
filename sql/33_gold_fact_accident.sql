CREATE TABLE gold.fact_accident (
    accident_key    TEXT PRIMARY KEY,
    date_key        INT NOT NULL REFERENCES gold.dim_date(date_key),
    station_id      TEXT NOT NULL REFERENCES gold.dim_station(station_id),
    location_key    BIGINT NOT NULL REFERENCES gold.dim_location(location_key),

    severity        SMALLINT,
    darkness_level  SMALLINT,
    distance_km     DOUBLE PRECISION,
    duration_minutes DOUBLE PRECISION,

    created_at      TIMESTAMPTZ DEFAULT now()
);