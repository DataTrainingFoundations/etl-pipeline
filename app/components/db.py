# ----------------------------------
# Imports
# ----------------------------------
import os
from sqlalchemy import create_engine, text
import streamlit as st
from pathlib import Path


# ==================================
# DATABASE ENGINE
# ==================================

@st.cache_resource
def get_engine():
    """
    Returns a cached SQLAlchemy engine
    using environment variables.
    """

    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")

    # Optional defaults (safe for Docker)
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")

    if not all([user, password, db]):
        raise RuntimeError("Database environment variables not set.")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

    return create_engine(connection_string)


# ==================================
# SQL DIRECTORY CONFIGURATION
# ==================================

# This path assumes docker-compose mounts:
#   ./sql  →  /sql
#
# Both:
#   - Postgres container (bootstrap)
#   - Streamlit container (runtime repair)
# share the same SQL source of truth.
SQL_DIR = Path("/sql")


# ==================================
# OBJECT → SQL FILE MAPPING
# ==================================

"""
Mapping between logical database objects and their
corresponding SQL definition files.

This allows:
- Targeted recreation of missing tables/views
- Self-healing during validation
- No full database reset required
- Single source of truth for schema definitions
"""

OBJECT_SQL_MAP = {
    # ===============================
    # BRONZE
    # ===============================
    "bronze.weather_daily": "10_weather_daily.sql",
    "bronze.stations": "11_bronze_stations.sql",
    "bronze.us_accidents": "12_bronze_us_accidents.sql",

    # ===============================
    # SILVER
    # ===============================
    "silver.us_accidents": "20_us_accidents.sql",
    "silver.stations": "21_silver_stations.sql",
    "silver.weather_daily": "22_silver_weather_daily.sql",
    "silver.accident_station_map": "23_silver_accident_station_map.sql",

    # ===============================
    # GOLD - DIMENSIONS
    # ===============================
    "gold.dim_date": "30_gold_dim_date.sql",
    "gold.dim_station": "31_gold_dim_station.sql",
    "gold.dim_location": "32_gold_dim_location.sql",

    # ===============================
    # GOLD - FACTS
    # ===============================
    "gold.fact_accident": "33_gold_fact_accident.sql",
    "gold.fact_weather_daily": "34_gold_fact_weather_daily.sql",
    "gold.fact_accident_station_daily": "35_gold_fact_accident_station_daily.sql",
}


# ==================================
# RUN SINGLE SQL FILE
# ==================================

def run_sql_file(filename: str):
    """
    Executes a single SQL file inside a transaction.

    Used for:
    - Recreating missing tables
    - Recreating materialized views
    - Repairing schema drift

    Execution is wrapped in engine.begin()
    so it runs inside a transaction.
    """

    engine = get_engine()
    file_path = SQL_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {file_path}"
        )

    sql = file_path.read_text()

    # Transactional execution
    with engine.begin() as conn:
        conn.execute(text(sql))


# ==================================
# ENSURE OBJECT EXISTS
# ==================================

def ensure_object_exists(object_name: str):
    """
    Ensures a specific database object exists.

    Workflow:
    1. Check if table exists.
    2. If it exists → do nothing.
    3. If missing → recreate using mapped SQL file.

    This function:
    - Does NOT drop existing objects
    - Does NOT validate contents
    - Only repairs missing schema objects

    Intended usage:
        ensure_object_exists("silver.weather_daily")
    """

    engine = get_engine()

    # Expect format: schema.object
    schema, name = object_name.split(".")

    # Check for existence using information_schema
    query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :name
        )
    """)

    with engine.begin() as conn:
        exists = conn.execute(
            query,
            {"schema": schema, "name": name}
        ).scalar()

    # If object already exists, nothing to do
    if exists:
        return

    # If object missing but no SQL mapping found → fail clearly
    if object_name not in OBJECT_SQL_MAP:
        raise ValueError(
            f"No SQL mapping found for {object_name}"
        )

    # Recreate object from SQL definition
    run_sql_file(OBJECT_SQL_MAP[object_name])
