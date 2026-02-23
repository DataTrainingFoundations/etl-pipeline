# ==================================
# Imports
# ==================================
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from components.db import get_engine


# ==================================
# Page Setup
# ==================================
st.set_page_config(layout="wide")
st.title("🗺 Accident Map by Severity")
st.caption("Accidents reconstructed from star schema (Fact + Spatial Dimension)")

engine = get_engine()
st.divider()


# ==================================
# Load Data
# ==================================
query = """
SELECT
    ST_Y(l.geom) AS latitude,
    ST_X(l.geom) AS longitude,
    f.severity
FROM gold.fact_accident f
JOIN gold.dim_location l
    ON f.location_key = l.location_key
WHERE l.geom IS NOT NULL
LIMIT 200000
"""

df = pd.read_sql(text(query), engine)

if df.empty:
    st.warning("No accident data found.")
    st.stop()

# 🔥 IMPORTANT FIX
df["severity"] = df["severity"].astype(str)


# ==================================
# Compute Map Center
# ==================================
center_lat = df["latitude"].mean()
center_lon = df["longitude"].mean()


# ==================================
# Plotly Map
# ==================================
fig = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    color="severity",
    color_discrete_map={
        "1": "#2ECC71",   # Minor
        "2": "#F1C40F",   # Moderate
        "3": "#E67E22",   # Serious
        "4": "#E74C3C"    # Severe
    },
    zoom=4,
    height=800,
    center={"lat": center_lat, "lon": center_lon},
    opacity=0.65,
)

fig.update_traces(marker=dict(size=6))

fig.update_layout(
    mapbox_style="carto-positron",
    margin=dict(l=0, r=0, t=0, b=0),
    legend_title_text="Severity"
)

st.plotly_chart(
    fig,
    use_container_width=True,
    config={
        "scrollZoom": True,
        "displayModeBar": False
    }
)