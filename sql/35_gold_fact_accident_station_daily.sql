CREATE TABLE gold.fact_accident_station_daily (
    date_key      INT NOT NULL REFERENCES gold.dim_date(date_key),
    station_id    TEXT NOT NULL REFERENCES gold.dim_station(station_id),

    accident_count INT,
    avg_severity   DOUBLE PRECISION,

    PRIMARY KEY (date_key, station_id)
);