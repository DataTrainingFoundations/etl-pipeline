# ==================================
# Imports
# ==================================
import streamlit as st
import time
from sqlalchemy import text

from pipeline.warehouse import run_all as run_warehouse
from pipeline.validators import validate_table
from components.db import get_engine
from components.table_explorer import render_table_explorer


# ==================================
# PAGE CONFIG
# ==================================
st.set_page_config(page_title="Run Warehouse", layout="wide")

st.title("🏗️ Build Warehouse (Gold Layer)")
st.caption("Construct dimensional star schema from Silver layer")

st.divider()

st.markdown("""
### Warehouse Objects Built
- dim_date
- dim_station
- dim_location
- fact_accident
- fact_weather_daily
- fact_accident_station_daily
""")

st.divider()


# ==================================
# BUILD CONTROLS
# ==================================
st.subheader("⚙️ Build Controls")

col1, col2 = st.columns([2, 1])

with col1:
    truncate = st.checkbox(
        "Truncate Gold tables before rebuild",
        value=False,
        help="Clears all Gold tables before rebuilding."
    )

with col2:
    run_button = st.button(
        "🚀 Run Warehouse Build",
        type="primary",
        use_container_width=True
    )


# ==================================
# EXECUTION
# ==================================
if run_button:

    start_time = time.perf_counter()
    status_placeholder = st.empty()

    try:
        with st.spinner("Building dimensional warehouse..."):
            result = run_warehouse(truncate=truncate)

        elapsed = time.perf_counter() - start_time

        status_placeholder.success("✅ Warehouse build completed")

        m1, m2 = st.columns(2)
        m1.metric("Execution Time (sec)", f"{elapsed:.2f}")
        m2.metric("Total Seconds Reported", f"{result['seconds']:.2f}")

        # ----------------------------------
        # Validate Core Tables
        # ----------------------------------
        engine = get_engine()

        validate_table(engine, "gold.dim_date", not_empty=True)
        validate_table(engine, "gold.fact_accident", not_empty=True)
        validate_table(engine, "gold.fact_weather_daily", not_empty=True)

        st.success("Core warehouse tables validated.")

    except Exception:
        import traceback
        status_placeholder.error("❌ Warehouse build failed")
        st.code(traceback.format_exc())


st.divider()


# ==================================
# WAREHOUSE EXPLORER
# ==================================
st.header("🏛 Warehouse Explorer")

# -----------------------------
# Dimensions
# -----------------------------
st.subheader("📐 Dimensions")

d1, d2, d3 = st.columns(3)

with d1:
    render_table_explorer(
        table_name="gold.dim_date",
        session_key="gold_dim_date",
        metric_label="dim_date Rows",
        allow_truncate=False,
    )

with d2:
    render_table_explorer(
        table_name="gold.dim_station",
        session_key="gold_dim_station",
        metric_label="dim_station Rows",
        allow_truncate=False,
    )

with d3:
    render_table_explorer(
        table_name="gold.dim_location",
        session_key="gold_dim_location",
        metric_label="dim_location Rows",
        allow_truncate=False,
    )

st.divider()

# -----------------------------
# Facts
# -----------------------------
st.subheader("📊 Fact Tables")

f1, f2, f3 = st.columns(3)

with f1:
    render_table_explorer(
        table_name="gold.fact_accident",
        session_key="gold_fact_accident",
        metric_label="fact_accident Rows",
        allow_truncate=False,
    )

with f2:
    render_table_explorer(
        table_name="gold.fact_weather_daily",
        session_key="gold_fact_weather",
        metric_label="fact_weather_daily Rows",
        allow_truncate=False,
    )

with f3:
    render_table_explorer(
        table_name="gold.fact_accident_station_daily",
        session_key="gold_fact_accident_station_daily",
        metric_label="fact_accident_station_daily Rows",
        allow_truncate=False,
    )