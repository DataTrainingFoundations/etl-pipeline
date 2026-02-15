# ==================================
# Imports
# ==================================
from pipeline.stations import run_all as run_stations
from pipeline.weather import run_all as run_weather
from pipeline.accidents import run_all as run_accidents

from pipeline.accident_station_map import build as run_station_map
from pipeline.weather_daily_pivot import build as run_weather_pivot
from pipeline.accident_weather import build as run_gold

from pipeline.validators import validate_table
from components.db import get_engine
from components.logger import get_logger


logger = get_logger(__name__)


# ==================================
# RUN FULL PIPELINE
# ==================================
def run_full(states: list[str]) -> dict:
    """
    Full DAG execution with validation.

    Execution Order:

    1. Stations
    2. Weather
    3. Accidents
    4. Weather Pivot
    5. Accident → Station Map
    6. Gold Build
    """

    engine = get_engine()

    logger.info("Starting FULL pipeline execution")

    # ==========================================================
    # INGESTION LAYER
    # ==========================================================

    # -----------------------------
    # Stations
    # -----------------------------
    logger.info("Running Stations")
    run_stations()

    validate_table(
        engine,
        "silver.stations",
        not_empty=True,
        required_columns=["station_id", "latitude", "longitude", "geom"],
    )

    # -----------------------------
    # Weather
    # -----------------------------
    logger.info("Running Weather")
    run_weather(states)

    validate_table(
        engine,
        "silver.weather_daily",
        not_empty=True,
        required_columns=["station_id", "obs_date", "element", "value"],
    )

    # -----------------------------
    # Accidents
    # -----------------------------
    logger.info("Running Accidents")
    run_accidents()

    validate_table(
        engine,
        "silver.us_accidents",
        not_empty=True,
        required_columns=["accident_id", "start_time", "geom"],
    )

    # ==========================================================
    # TRANSFORMATION LAYER
    # ==========================================================

    # -----------------------------
    # Weather Pivot
    # -----------------------------
    logger.info("Refreshing Weather Pivot")
    run_weather_pivot()

    validate_table(
        engine,
        "silver.weather_daily_pivot",
        not_empty=True,
    )

    # -----------------------------
    # Accident → Station Map
    # -----------------------------
    logger.info("Building Accident → Station Map")
    run_station_map()

    validate_table(
        engine,
        "silver.accident_station_map",
        not_empty=True,
    )

    # ==========================================================
    # GOLD LAYER
    # ==========================================================

    logger.info("Building Gold accident_weather")
    run_gold()

    validate_table(
        engine,
        "gold.accident_weather",
        not_empty=True,
    )

    logger.info("FULL pipeline execution completed successfully")

    return {"status": "success"}
