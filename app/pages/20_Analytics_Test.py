# ==================================
# Imports
# ==================================
import streamlit as st
import pandas as pd
import pydeck as pdk
from sqlalchemy import text
from components.db import get_engine


# ==================================
# Page Setup
# ==================================
st.set_page_config(layout="wide")
st.title("🗺 Accident Severity Map (PyDeck)")
st.caption("Meter-based radius scaling (true zoom behavior)")

engine = get_engine()

st.divider()


# ==================================
# Load States
# ==================================
states_query = """
SELECT DISTINCT state
FROM gold.dim_location
ORDER BY state
"""
states = pd.read_sql(text(states_query), engine)["state"].tolist()

default_states = ["GA"] if "GA" in states else []


# ==================================
# Controls
# ==================================
col1, col2 = st.columns(2)

with col1:
    selected_states = st.multiselect(
        "Select State(s)",
        states,
        default=default_states
    )

with col2:
    sample_size = st.slider(
        "Sample Size",
        1000, 100000, 10000, step=1000
    )

st.divider()


# ==================================
# Load Data
# ==================================
query = """
SELECT
    ST_X(l.geom) AS longitude,
    ST_Y(l.geom) AS latitude,
    f.severity,
    l.state
FROM gold.fact_accident f
JOIN gold.dim_location l
    ON f.location_key = l.location_key
WHERE l.geom IS NOT NULL
"""

params = {}

if selected_states:
    query += " AND l.state = ANY(:states)"
    params["states"] = selected_states

query += " ORDER BY random() LIMIT :limit"
params["limit"] = sample_size

df = pd.read_sql(text(query), engine, params=params)

if df.empty:
    st.warning("No accident data found.")
    st.stop()

st.write(f"Displaying {len(df):,} accidents")


# ==================================
# Map Center
# ==================================
center_lat = df["latitude"].mean()
center_lon = df["longitude"].mean()


# ==================================
# Severity Color Logic
# ==================================
# RGB mapping based on severity
def severity_color(sev):
    if sev == 4:
        return [255, 0, 0]       # red
    elif sev == 3:
        return [255, 140, 0]     # orange
    elif sev == 2:
        return [255, 215, 0]     # yellow
    else:
        return [0, 200, 255]     # blue


df["color"] = df["severity"].apply(severity_color)


# ==================================
# PyDeck Layer (Meter-Based Scaling)
# ==================================
layer = pdk.Layer(
    "ScatterplotLayer",
    df,
    get_position='[longitude, latitude]',
    get_fill_color='color',
    get_radius="severity * 30",   # <-- meters, scales naturally
    pickable=True,
    opacity=0.5,
)


# ==================================
# View State
# ==================================
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=6,
    pitch=0,
)


# ==================================
# Render Deck
# ==================================
deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style="mapbox://styles/mapbox/dark-v9",
    tooltip={
        "text": "Severity: {severity}\nState: {state}"
    }
)

st.pydeck_chart(deck)