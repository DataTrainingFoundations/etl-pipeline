# 🌦 Weather + Accidents ETL Pipeline

A production-style data engineering pipeline integrating:

-   🚗 US Accidents dataset (7.7M+ records)
-   🌦 NOAA GHCN Daily weather data
-   📍 Geospatial nearest-station mapping (PostGIS)
-   🏗 Bronze → Silver → Gold layered architecture
-   🐳 Fully containerized with Docker
-   📊 Interactive UI built with Streamlit

------------------------------------------------------------------------

## 🧠 Project Overview

This project builds a scalable ETL pipeline that:

1.  Ingests raw US accident data (Kaggle dataset)
2.  Downloads and processes NOAA daily weather station files
3.  Maps each accident to its nearest weather station
4.  Joins accident + weather data into an analytics-ready gold layer

The goal:\
Enable weather impact analysis on traffic accidents using clean,
structured, geospatially-aware data.

------------------------------------------------------------------------

## 🏗 Architecture

### 🔹 Bronze Layer (Raw Data)

  Table                  Description
  ---------------------- -----------------------------------
  bronze.stations        Raw NOAA station metadata
  bronze.weather_daily   Raw parsed NOAA .dly weather data
  bronze.us_accidents    Raw US accidents dataset

------------------------------------------------------------------------

### 🔹 Silver Layer (Clean + Structured)

  Table                         Description
  ----------------------------- -------------------------------------
  silver.stations               Cleaned station metadata + geometry
  silver.weather_daily          Cleaned weather records
  silver.weather_daily_pivot    Materialized daily pivot
  silver.us_accidents           Cleaned accident records + geometry
  silver.accident_station_map   Nearest station mapping

------------------------------------------------------------------------

### 🔹 Gold Layer (Analytics Ready)

  Table                   Description
  ----------------------- ----------------------------------------------
  gold.accident_weather   Final fact table joining accidents + weather

------------------------------------------------------------------------

## 🗺 Geospatial Logic

Each accident is mapped to its nearest weather station using:

``` sql
ORDER BY a.geom <-> geom
LIMIT 1
```

With a GiST spatial index:

``` sql
CREATE INDEX idx_silver_stations_geom
ON silver.stations USING GIST (geom);
```

------------------------------------------------------------------------

## ⚡ Performance

-   7.7M+ accident rows processed
-   Parallel NOAA downloads
-   Multi-threaded COPY ingestion
-   Materialized weather pivot
-   Spatial indexing

------------------------------------------------------------------------

## 🐳 Tech Stack

-   Python 3.11
-   PostgreSQL 16
-   PostGIS
-   SQLAlchemy
-   Pandas
-   Streamlit
-   Docker Compose

------------------------------------------------------------------------

## 🚀 Running the Project

### 1️⃣ Clone the repository

``` bash
git clone https://github.com/DataTrainingFoundations/etl-pipeline
cd weather-accident-etl
```

### 2️⃣ Start containers

``` bash
docker compose up --build
```

### 3️⃣ Open Streamlit UI

    http://localhost:8501

------------------------------------------------------------------------

## 📥 Data Sources

### 🚗 US Accidents (Kaggle)

Dataset:\
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

You have **two options** for ingestion:

#### Option 1 --- Manual Download (Simple)

1.  Download the dataset directly from Kaggle.
2.  Extract the CSV.
3.  Place the file into:
    data/landing/accidents/

------------------------------------------------------------------------

#### Option 2 --- Automated Kaggle Download (Recommended)

You can configure Kaggle API credentials for automated downloading.

1.  Create a Kaggle API Key from your Kaggle account.

2.  Place your `kaggle` credentials in:
   .env

3.  Replace the following in `.env` file:

    KAGGLE_USERNAME=your_username
    KAGGLE_KEY=your_key

> 🔴 **Note:**\
> The project currently defaults to a dummy/test Kaggle account
> configured in the environment, so it should work out of the box.\
> For production or personal use, you should replace these credentials
> with your own.

------------------------------------------------------------------------

### 🌦 NOAA GHCN Daily Weather

Weather data is downloaded automatically by the pipeline from:

    https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/

The downloader: - Selects stations from `silver.stations` - Filters by
selected states - Applies date range restrictions - Saves parsed daily
files into:

    data/landing/weather/


------------------------------------------------------------------------

## 🎯 Key Engineering Concepts

-   Layered data modeling (Bronze/Silver/Gold)
-   Idempotent pipeline design
-   Conflict-safe upserts
-   Materialized views
-   Geospatial nearest-neighbor joins
-   Parallel ingestion
-   Containerized reproducibility

------------------------------------------------------------------------

Built as a portfolio data engineering project demonstrating scalable ETL
architecture and geospatial analytics.
