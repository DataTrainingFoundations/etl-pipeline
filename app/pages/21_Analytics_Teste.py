# ==================================
# Imports
# ==================================
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import text
from components.db import get_engine


# ==================================
# Page Setup
# ==================================
st.set_page_config(layout="wide")
st.title("🌧 Weather Impact on Accident Severity")
st.caption("Scatter + Linear Trend Analysis of Precipitation vs Severity")

engine = get_engine()
st.divider()


# ==================================
# Load Data
# ==================================
query = """
SELECT
    a.severity,
    w.prcp_mm
FROM gold.fact_accident a
JOIN gold.fact_weather_daily w
    ON a.date_key = w.date_key
   AND a.station_id = w.station_id
WHERE w.prcp_mm IS NOT NULL
LIMIT 200000
"""

df = pd.read_sql(text(query), engine)

if df.empty:
    st.warning("No data found.")
    st.stop()


# ==================================
# Cleaning
# ==================================
df = df[df["severity"].notnull()]
df = df[df["prcp_mm"] >= 0]


# ==================================
# Correlation
# ==================================
correlation = df["prcp_mm"].corr(df["severity"])
st.metric("Correlation (Precipitation vs Severity)", round(correlation, 4))


# ==================================
# Scatter + Linear Regression
# ==================================
plt.figure(figsize=(10, 6))

sns.regplot(
    data=df,
    x="prcp_mm",
    y="severity",
    scatter_kws={
        "alpha": 0.08,
        "s": 10
    },
    line_kws={
        "color": "red",
        "linewidth": 2
    }
)

plt.xlabel("Precipitation (mm)")
plt.ylabel("Accident Severity")
plt.title("Precipitation vs Accident Severity (Linear Trend)")

st.pyplot(plt)
plt.clf()


# ==================================
# Log View (Handles Skew)
# ==================================
st.subheader("Log-Scaled Precipitation View")

df["prcp_log"] = np.log1p(df["prcp_mm"])

plt.figure(figsize=(10, 6))

sns.regplot(
    data=df,
    x="prcp_log",
    y="severity",
    scatter_kws={
        "alpha": 0.08,
        "s": 10
    },
    line_kws={
        "color": "darkblue",
        "linewidth": 2
    }
)

plt.xlabel("Log(Precipitation + 1)")
plt.ylabel("Accident Severity")
plt.title("Log-Scaled Rainfall vs Severity")

st.pyplot(plt)
plt.clf()