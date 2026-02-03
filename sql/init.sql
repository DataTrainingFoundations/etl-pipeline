-- sql/init.sql
-- ETL bootstrap: bronze (raw) -> silver (clean) -> gold (analytics)

BEGIN;

-- -------------------------
-- 0) SCHEMAS
-- -------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- -------------------------
-- 1) INGESTION LOG (file/run tracking)
-- -------------------------
CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
    run_id          UUID PRIMARY KEY,
    dataset         TEXT NOT NULL,          -- 'weather' | 'accidents'
    source_file     TEXT,
    status          TEXT NOT NULL,          -- 'SUCCESS' | 'FAILED' | 'QUARANTINED'
    rows_read       INTEGER,
    rows_loaded     INTEGER,
    rows_rejected   INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_dataset_created
    ON bronze.ingestion_log (dataset, created_at DESC);

-- -------------------------
-- 2) BRONZE TABLES (raw, loose types)
-- -------------------------
CREATE TABLE IF NOT EXISTS bronze.weather (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT now(),

    event_time      TEXT,
    station_id      TEXT,
    lat             TEXT,
    lon             TEXT,

    temperature     TEXT,
    precipitation   TEXT,
    wind_speed      TEXT,
    visibility      TEXT,
    conditions      TEXT,

    payload         JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_weather_run_id
    ON bronze.weather (run_id);

CREATE TABLE IF NOT EXISTS bronze.accidents (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT now(),

    accident_time   TEXT,
    accident_id     TEXT,
    severity        TEXT,
    lat             TEXT,
    lon             TEXT,
    city            TEXT,
    state           TEXT,

    description     TEXT,
    payload         JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_accidents_run_id
    ON bronze.accidents (run_id);

-- -------------------------
-- 3) REJECTED RECORDS (row-level rejects)
-- -------------------------
CREATE TABLE IF NOT EXISTS bronze.rejected_weather (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    source_file     TEXT,
    rejected_at     TIMESTAMPTZ DEFAULT now(),
    reason          TEXT NOT NULL,
    payload         JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.rejected_accidents (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    source_file     TEXT,
    rejected_at     TIMESTAMPTZ DEFAULT now(),
    reason          TEXT NOT NULL,
    payload         JSONB NOT NULL
);

-- -------------------------
-- 4) SILVER TABLES (cleaned + typed)
-- -------------------------
CREATE TABLE IF NOT EXISTS silver.weather (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    source_file         TEXT,
    ingested_at         TIMESTAMPTZ,

    event_ts            TIMESTAMPTZ NOT NULL,
    station_id          TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,

    temperature_c       DOUBLE PRECISION,
    precipitation_mm    DOUBLE PRECISION,
    wind_speed_mps      DOUBLE PRECISION,
    visibility_km       DOUBLE PRECISION,
    conditions          TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_weather_event_ts
    ON silver.weather (event_ts);

CREATE TABLE IF NOT EXISTS silver.accidents (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ,

    accident_ts     TIMESTAMPTZ NOT NULL,
    accident_id     TEXT,
    severity        INTEGER,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    city            TEXT,
    state           TEXT,
    description     TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_accidents_accident_ts
    ON silver.accidents (accident_ts);

-- -------------------------
-- 5) GOLD TABLE (final analytics dataset)
-- -------------------------
CREATE TABLE IF NOT EXISTS gold.weather_accident_impact (
    id                  BIGSERIAL PRIMARY KEY,

    bucket_time         TIMESTAMPTZ NOT NULL,
    bucket_geo          TEXT NOT NULL,

    accident_count      INTEGER NOT NULL,
    avg_severity        DOUBLE PRECISION,

    avg_temp_c          DOUBLE PRECISION,
    total_precip_mm     DOUBLE PRECISION,
    avg_wind_speed_mps  DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_gold_impact_bucket
    ON gold.weather_accident_impact (bucket_time, bucket_geo);

COMMIT;
