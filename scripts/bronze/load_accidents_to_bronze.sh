#!/usr/bin/env bash
set -euo pipefail

DB_USER="etl"
DB_NAME="etl_db"
ACC="/work/data/landing/accidents/US_Accidents_March23.csv"

echo "Loading accidents from: $ACC"

docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" <<SQL
CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.accidents_raw;
CREATE TABLE bronze.accidents_raw (
  id TEXT,
  source TEXT,
  severity INT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  start_lat DOUBLE PRECISION,
  start_lng DOUBLE PRECISION,
  end_lat DOUBLE PRECISION,
  end_lng DOUBLE PRECISION,
  distance_mi DOUBLE PRECISION,
  description TEXT,
  street TEXT,
  city TEXT,
  county TEXT,
  state TEXT,
  zipcode TEXT,
  country TEXT,
  timezone TEXT,
  airport_code TEXT,
  weather_timestamp TIMESTAMP,
  temperature_f DOUBLE PRECISION,
  wind_chill_f DOUBLE PRECISION,
  humidity DOUBLE PRECISION,
  pressure_in DOUBLE PRECISION,
  visibility_mi DOUBLE PRECISION,
  wind_direction TEXT,
  wind_speed_mph DOUBLE PRECISION,
  precipitation_in DOUBLE PRECISION,
  weather_condition TEXT,
  amenity BOOLEAN,
  bump BOOLEAN,
  crossing BOOLEAN,
  give_way BOOLEAN,
  junction BOOLEAN,
  no_exit BOOLEAN,
  railway BOOLEAN,
  roundabout BOOLEAN,
  station BOOLEAN,
  stop BOOLEAN,
  traffic_calming BOOLEAN,
  traffic_signal BOOLEAN,
  turning_loop BOOLEAN,
  sunrise_sunset TEXT,
  civil_twilight TEXT,
  nautical_twilight TEXT,
  astronomical_twilight TEXT
);

\\copy bronze.accidents_raw FROM '$ACC' CSV HEADER;

SELECT COUNT(*) AS accident_rows FROM bronze.accidents_raw;
SQL

echo "✅ Done."

