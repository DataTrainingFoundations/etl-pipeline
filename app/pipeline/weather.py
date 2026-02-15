# ==================================
# Imports
# ==================================
from pathlib import Path
from datetime import date
import csv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from sqlalchemy import text

from pipeline.validators import validate_table
from components.db import get_engine
from components.logger import get_logger

logger = get_logger(__name__)


# ==================================
# Constants
# ==================================
BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all"

LANDING_DIR = Path("/data/landing/weather")
ARCHIVE_DIR = Path("/data/archive/weather")

LANDING_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


# ==================================
# DOWNLOAD
# ==================================
def download(
    states: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    max_workers: int = 12,
):
    """
    Download NOAA .dly files.

    Behavior:
        states=None  → download ALL stations
        states=[]    → skip download
        states=[...] → filter by state list
    """

    engine = get_engine()

    # -----------------------------
    # Fetch station_ids
    # -----------------------------
    if states is None:
        logger.info("Downloading weather for ALL stations")
        station_ids = pd.read_sql(
            text("SELECT station_id FROM silver.stations"),
            engine,
        )["station_id"].tolist()

    elif len(states) == 0:
        logger.info("Empty state list provided — skipping weather download")
        return {"downloaded": 0}

    else:
        logger.info(f"Downloading weather for states: {states}")

        placeholders = ",".join([f":s{i}" for i in range(len(states))])
        query = text(f"""
            SELECT station_id
            FROM silver.stations
            WHERE state IN ({placeholders})
        """)
        params = {f"s{i}": s for i, s in enumerate(states)}

        station_ids = pd.read_sql(query, engine, params=params)["station_id"].tolist()

    if not station_ids:
        logger.warning("No station_ids found for weather download")
        return {"downloaded": 0}

    if start_date is None:
        start_date = date(2015, 1, 1)
    if end_date is None:
        end_date = date.today()

    downloaded = 0

    def download_station(station_id: str):
        nonlocal downloaded

        out_csv = LANDING_DIR / f"{station_id}.csv"
        if out_csv.exists():
            return

        url = f"{BASE_URL}/{station_id}.dly"
        r = requests.get(url, timeout=60)
        r.raise_for_status()

        with open(out_csv, "w", newline="") as fout:
            writer = csv.writer(fout)

            writer.writerow([
                "station_id",
                "obs_date",
                "element",
                "value",
                "m_flag",
                "q_flag",
                "s_flag",
            ])

            for line in r.text.splitlines():
                station = line[0:11].strip()
                year = int(line[11:15])
                month = int(line[15:17])
                element = line[17:21]

                for day in range(1, 32):
                    base = 21 + (day - 1) * 8
                    value = line[base:base+5].strip()

                    if value == "-9999":
                        continue

                    try:
                        obs_date = date(year, month, day)
                    except ValueError:
                        continue

                    if not (start_date <= obs_date <= end_date):
                        continue

                    writer.writerow([
                        station,
                        obs_date.isoformat(),
                        element,
                        int(value),
                        line[base+5].strip() or None,
                        line[base+6].strip() or None,
                        line[base+7].strip() or None,
                    ])

        downloaded += 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_station, sid) for sid in station_ids]
        for f in as_completed(futures):
            f.result()

    logger.info(f"Downloaded {downloaded} weather station files")

    return {"downloaded": downloaded}


# ==================================
# INGEST → BRONZE
# ==================================
def ingest(files: list[Path] | None = None, max_workers: int = 4):

    engine = get_engine()

    validate_table(engine, "bronze.weather_daily", not_empty=False)

    if files is None:
        files = list(LANDING_DIR.glob("*.csv"))

    if not files:
        logger.info("No weather files found for ingest")
        return {"rows_inserted": 0}

    total_rows = 0

    def worker(file: Path) -> int:
        try:
            raw_conn = engine.raw_connection()
            try:
                cur = raw_conn.cursor()
                try:
                    with open(file, "r") as f:
                        cur.copy_expert("""
                            COPY bronze.weather_daily (
                                station_id,
                                obs_date,
                                element,
                                value,
                                m_flag,
                                q_flag,
                                s_flag
                            )
                            FROM STDIN
                            WITH (FORMAT CSV, HEADER TRUE)
                        """, f)

                    raw_conn.commit()
                finally:
                    cur.close()
            finally:
                raw_conn.close()

            with open(file, "r") as f:
                row_count = sum(1 for _ in f) - 1

            file.rename(ARCHIVE_DIR / file.name)

            return row_count

        except Exception as e:
            logger.error(f"Failed ingest for {file.name}: {e}")
            return 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, f) for f in files]
        for future in as_completed(futures):
            total_rows += future.result()

    validate_table(
        engine,
        "bronze.weather_daily",
        not_empty=True,
        required_columns=["station_id", "obs_date", "element", "value"],
    )

    logger.info(f"Inserted {total_rows:,} rows into bronze.weather_daily")

    return {"rows_inserted": total_rows}


# ==================================
# TRANSFORM → SILVER
# ==================================
def transform(truncate: bool = False) -> dict:

    engine = get_engine()

    validate_table(
        engine,
        "bronze.weather_daily",
        not_empty=True,
        required_columns=["station_id", "obs_date", "element", "value"],
    )

    with engine.begin() as conn:

        if truncate:
            logger.info("Truncating silver.weather_daily")
            conn.execute(text("TRUNCATE TABLE silver.weather_daily"))

        result = conn.execute(text("""
            INSERT INTO silver.weather_daily (
                station_id,
                obs_date,
                element,
                value
            )
            SELECT
                station_id,
                obs_date::DATE,
                element,
                value::DOUBLE PRECISION
            FROM bronze.weather_daily
            ON CONFLICT (station_id, obs_date, element)
            DO UPDATE SET value = EXCLUDED.value;
        """))

        rows_written = result.rowcount

    validate_table(
        engine,
        "silver.weather_daily",
        not_empty=True,
        required_columns=["station_id", "obs_date", "element", "value"],
    )

    logger.info(
        f"Transformed {rows_written:,} rows "
        f"bronze.weather_daily → silver.weather_daily"
    )

    return {"rows_written": rows_written}


# ==================================
# RUN ALL
# ==================================
def run_all(states: list[str] | None = None):

    download_result = download(states)
    ingest_result = ingest()
    transform_result = transform()

    return {
        "download": download_result,
        "ingest": ingest_result,
        "transform": transform_result,
    }
