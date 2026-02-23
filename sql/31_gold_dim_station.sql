CREATE TABLE gold.dim_station (
    station_id     TEXT PRIMARY KEY,
    state          CHAR(2),
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION
);