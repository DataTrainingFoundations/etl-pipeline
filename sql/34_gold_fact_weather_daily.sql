CREATE TABLE gold.fact_weather_daily (
    date_key     INT NOT NULL REFERENCES gold.dim_date(date_key),
    station_id   TEXT NOT NULL REFERENCES gold.dim_station(station_id),

    tmax_c       DOUBLE PRECISION,
    tmin_c       DOUBLE PRECISION,
    prcp_mm      DOUBLE PRECISION,
    snow_mm      DOUBLE PRECISION,

    PRIMARY KEY (date_key, station_id)
);