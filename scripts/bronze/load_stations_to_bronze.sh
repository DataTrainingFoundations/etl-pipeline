#!/usr/bin/env bash
set -euo pipefail

DB_USER="etl"
DB_NAME="etl_db"
STATIONS="/work/data/landing/ghcn_meta/ghcnd-stations.csv"

echo "Loading stations from: $STATIONS"

docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" <<SQL
CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.stations;
CREATE TABLE bronze.stations (
  id TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  elevation DOUBLE PRECISION,
  state TEXT,
  name TEXT,
  gsn_flag TEXT,
  hcn_crn_flag TEXT,
  wmo_id TEXT
);

\\copy bronze.stations FROM '$STATIONS' CSV HEADER;

SELECT COUNT(*) AS station_rows FROM bronze.stations;
SQL

echo "✅ Done."

