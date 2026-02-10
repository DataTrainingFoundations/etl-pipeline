#!/usr/bin/env bash
set -euo pipefail

DB=etl_db
USER=etl
SERVICE=postgres

# 1) Ensure schema + table exist (so script works on fresh DB too)
docker compose exec -T $SERVICE psql -U $USER -d $DB <<'SQL'
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ghcn_daily_raw (
  station_id TEXT,
  obs_date   DATE,
  element    TEXT,
  value      INTEGER,
  m_flag     TEXT,
  q_flag     TEXT,
  s_flag     TEXT,
  obs_time   TEXT
);
SQL

# 2) Now we can safely truncate before reloading
echo "⚠️ Truncating bronze.ghcn_daily_raw before reload"
docker compose exec -T $SERVICE psql -U $USER -d $DB -c \
"TRUNCATE bronze.ghcn_daily_raw;"

# 3) Load each year file into the table
for y in {2016..2023}; do
  FILE="/work/data/landing/ghcn_by_year_2016_2023/${y}.csv.gz"
  echo "📥 Loading GHCN year $y from $FILE ..."

  docker compose exec -T $SERVICE psql -U $USER -d $DB -c "\
\copy bronze.ghcn_daily_raw(
  station_id,
  obs_date,
  element,
  value,
  m_flag,
  q_flag,
  s_flag,
  obs_time
) FROM PROGRAM 'gzip -dc $FILE'
WITH (FORMAT csv, HEADER false);"
done

echo "✅ All GHCN years loaded into bronze.ghcn_daily_raw"

