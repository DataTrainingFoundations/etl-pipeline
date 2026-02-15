# ==================================
# Imports
# ==================================
from pipeline.stations import run_all as run_stations
from pipeline.weather import run_all as run_weather
from pipeline.accidents import run_all as run_accidents

from pipeline.weather_daily_pivot import build as build_weather_pivot
from pipeline.accident_station_map import build as build_station_map
from pipeline.accident_weather import build as build_gold

from pipeline.validators import validate_table
from components.db import get_engine
from components.logger import get_logger


logger = get_logger(__name__)


# ==================================
# RUN FULL PIPELINE
# ==================================
def run_full(states: list[str] | None = None) -> dict:
    """
    Execute full pipeline DAG.

    Order:
        1. Stations
        2. Weather
        3. Accidents
        4. Weather Daily Pivot
        5. Accident → Station Map
        6. Gold (Accident Weather)

    Args:
        states: Optional list of state codes to filter weather ingestion.
                If None, all states are processed.

    Returns:
        dict with status
    """

    engine = get_engine()

    logger.info("Starting FULL pipeline execution")

    # ==========================================================
    # INGEST LAYER
    # ==========================================================

    # -----------------------------
    # Stations
    # -----------------------------
    logger.info("Running stations")
    run_stations()

    validate_table(
        engine,
        "silver.stations",
        not_empty=True,
        required_columns=[
            "station_id",
            "latitude",
            "longitude",
            "geom",
        ],
    )

    # -----------------------------
    # Weather
    # -----------------------------
    logger.info("Running weather")
    run_weather(states)

    validate_table(
        engine,
        "silver.weather_daily",
        not_empty=True,
    )

    # -----------------------------
    # Accidents
    # -----------------------------
    logger.info("Running accidents")
    run_accidents()

    validate_table(
        engine,
        "silver.us_accidents",
        not_empty=True,
    )

    # ==========================================================
    # TRANSFORM LAYER
    # ==========================================================

    # -----------------------------
    # Weather Daily Pivot
    # -----------------------------
    logger.info("Refreshing weather_daily_pivot")
    build_weather_pivot()

    validate_table(
        engine,
        "silver.weather_daily_pivot",
        not_empty=True,
    )

    # -----------------------------
    # Accident → Station Map
    # -----------------------------
    logger.info("Building accident_station_map")
    build_station_map()

    validate_table(
        engine,
        "silver.accident_station_map",
        not_empty=True,
    )

    # ==========================================================
    # GOLD LAYER
    # ==========================================================

    logger.info("Building gold.accident_weather")
    build_gold()

    validate_table(
        engine,
        "gold.accident_weather",
        not_empty=True,
    )

    logger.info("FULL pipeline execution completed successfully")

    return {"status": "success"}
