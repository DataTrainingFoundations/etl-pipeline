# ==================================
# Imports
# ==================================
import time
from sqlalchemy import text

from components.db import get_engine
from pipeline.validators import validate_table
from components.logger import get_logger

logger = get_logger(__name__)


# ==================================
# VALIDATE SILVER DEPENDENCIES
# ==================================
def validate_dependencies():
    engine = get_engine()

    validate_table(engine, "silver.us_accidents", not_empty=True)
    validate_table(engine, "silver.weather_daily", not_empty=True)
    validate_table(engine, "silver.stations", not_empty=True)
    validate_table(engine, "silver.accident_station_map", not_empty=True)


# ==================================
# TRUNCATE GOLD (SAFE ORDER)
# ==================================
def truncate_gold():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Truncating gold schema (safe order)")

        conn.execute(text("TRUNCATE TABLE gold.fact_accident_station_daily"))
        conn.execute(text("TRUNCATE TABLE gold.fact_weather_daily"))
        conn.execute(text("TRUNCATE TABLE gold.fact_accident"))
        conn.execute(text("TRUNCATE TABLE gold.dim_location"))
        conn.execute(text("TRUNCATE TABLE gold.dim_station"))
        conn.execute(text("TRUNCATE TABLE gold.dim_date"))


# ==================================
# BUILD DIMENSIONS
# ==================================
def build_dim_date():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Building dim_date")

        conn.execute(text("""
            INSERT INTO gold.dim_date (
                date_key,
                full_date,
                year,
                quarter,
                month,
                day,
                is_weekend
            )
            SELECT
                EXTRACT(YEAR FROM d)::INT * 10000 +
                EXTRACT(MONTH FROM d)::INT * 100 +
                EXTRACT(DAY FROM d)::INT,
                d,
                EXTRACT(YEAR FROM d),
                EXTRACT(QUARTER FROM d),
                EXTRACT(MONTH FROM d),
                EXTRACT(DAY FROM d),
                CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7)
                     THEN TRUE ELSE FALSE END
            FROM (
                SELECT DISTINCT DATE(start_time) AS d
                FROM silver.us_accidents
                UNION
                SELECT DISTINCT obs_date AS d
                FROM silver.weather_daily
            ) dates;
        """))


def build_dim_station():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Building dim_station")

        conn.execute(text("""
            INSERT INTO gold.dim_station (
                station_id,
                state,
                latitude,
                longitude
            )
            SELECT
                station_id,
                state,
                latitude,
                longitude
            FROM silver.stations;
        """))


def build_dim_location():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Building dim_location")

        conn.execute(text("""
            INSERT INTO gold.dim_location (
                state,
                county,
                city,
                geom
            )
            SELECT DISTINCT
                state,
                county,
                city,
                geom::geometry(Point, 4326)
            FROM silver.us_accidents
            WHERE geom IS NOT NULL;
        """))


# ==================================
# BUILD FACT TABLES
# ==================================
def build_fact_accident():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Building fact_accident")

        conn.execute(text("""
            INSERT INTO gold.fact_accident (
                accident_key,
                date_key,
                station_id,
                location_key,
                severity,
                darkness_level,
                distance_km,
                duration_minutes
            )
            SELECT
                a.accident_id,
                dd.date_key,
                m.station_id,
                dl.location_key,
                a.severity,
                a.darkness_level,
                m.distance_km,
                a.duration_minutes
            FROM silver.us_accidents a
            JOIN silver.accident_station_map m
                ON a.accident_id = m.accident_id
            JOIN gold.dim_date dd
                ON dd.full_date = DATE(a.start_time)
            JOIN gold.dim_location dl
                ON dl.state = a.state
                AND dl.county = a.county
                AND dl.city = a.city
                AND dl.geom = a.geom::geometry(Point, 4326);
        """))


def build_fact_weather_daily():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Building fact_weather_daily")

        conn.execute(text("""
            INSERT INTO gold.fact_weather_daily (
                date_key,
                station_id,
                tmax_c,
                tmin_c,
                prcp_mm,
                snow_mm
            )
            SELECT
                dd.date_key,
                w.station_id,
                MAX(CASE WHEN w.element = 'TMAX' THEN w.value END),
                MAX(CASE WHEN w.element = 'TMIN' THEN w.value END),
                MAX(CASE WHEN w.element = 'PRCP' THEN w.value END),
                MAX(CASE WHEN w.element = 'SNOW' THEN w.value END)
            FROM silver.weather_daily w
            JOIN gold.dim_date dd
                ON dd.full_date = w.obs_date
            GROUP BY dd.date_key, w.station_id;
        """))


def build_fact_accident_station_daily():
    engine = get_engine()

    with engine.begin() as conn:
        logger.info("Building fact_accident_station_daily")

        conn.execute(text("""
            INSERT INTO gold.fact_accident_station_daily (
                date_key,
                station_id,
                accident_count,
                avg_severity
            )
            SELECT
                date_key,
                station_id,
                COUNT(*),
                AVG(severity)
            FROM gold.fact_accident
            GROUP BY date_key, station_id;
        """))


# ==================================
# ORCHESTRATOR
# ==================================
def run_all(truncate: bool = True):

    start_time = time.perf_counter()

    validate_dependencies()

    if truncate:
        truncate_gold()

    build_dim_date()
    build_dim_station()
    build_dim_location()

    build_fact_accident()
    build_fact_weather_daily()

    build_fact_accident_station_daily()

    elapsed = time.perf_counter() - start_time

    logger.info(f"Warehouse build completed in {elapsed:.2f} seconds")

    return {
        "seconds": round(elapsed, 2)
    }