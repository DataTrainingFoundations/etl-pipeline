# ==================================
# Imports
# ==================================
import time
from sqlalchemy import text

from components.db import get_engine
from components.logger import get_logger
from pipeline.validators import validate_table


logger = get_logger(__name__)


# ==================================
# BUILD MATERIALIZED VIEW
# ==================================
def build(concurrent: bool = False) -> dict:
    """
    Refresh silver.weather_daily_pivot materialized view.

    Args:
        concurrent: If True, uses CONCURRENTLY (requires unique index)

    Returns:
        dict with rows_refreshed and seconds
    """

    engine = get_engine()
    start_time = time.perf_counter()

    logger.info(
        f"Refreshing weather_daily_pivot | concurrent={concurrent}"
    )

    concurrent_clause = "CONCURRENTLY" if concurrent else ""

    refresh_sql = f"""
        REFRESH MATERIALIZED VIEW {concurrent_clause}
        silver.weather_daily_pivot;
    """

    try:
        # Refresh view
        with engine.begin() as conn:
            conn.execute(text(refresh_sql))

        # Validate exists + not empty
        validate_table(
            engine,
            "silver.weather_daily_pivot",
            not_empty=True,
        )

        # Get row count
        with engine.begin() as conn:
            row_count = conn.execute(
                text("SELECT COUNT(*) FROM silver.weather_daily_pivot")
            ).scalar()

        elapsed = time.perf_counter() - start_time

        logger.info(
            f"weather_daily_pivot refreshed successfully | rows={row_count} | seconds={elapsed:.2f}"
        )

        return {
            "rows_refreshed": row_count,
            "seconds": elapsed,
        }

    except Exception as e:
        logger.exception("weather_daily_pivot refresh failed")
        raise