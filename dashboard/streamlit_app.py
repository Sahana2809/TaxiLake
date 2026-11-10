"""Dashboard over the trip warehouse -- the free stand-in for QuickSight.

Run:
  source .venv/bin/activate
  streamlit run dashboard/streamlit_app.py
"""
import os
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="NYC Taxi Warehouse", layout="wide")


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="localhost",
        port=os.environ["POSTGRES_PORT"],
    )


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, get_connection())


st.title("NYC Taxi Warehouse")
st.caption(
    "Postgres star schema, loaded from a MinIO-backed data lake cleaned by Spark. "
    "Local stand-in for a QuickSight dashboard on top of Redshift."
)

kpis = run_query("""
    SELECT
        COUNT(*) AS trips,
        ROUND(SUM(total_amount)) AS revenue,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM fact_trips
""").iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total trips", f"{int(kpis['trips']):,}")
col2.metric("Total revenue", f"${kpis['revenue']:,.0f}")
col3.metric("Avg fare", f"${kpis['avg_fare']:.2f}")
col4.metric("Avg trip distance", f"{kpis['avg_distance']:.2f} mi")

st.subheader("Daily trips and revenue")
daily = run_query("""
    SELECT d.full_date, COUNT(*) AS trips, ROUND(SUM(f.total_amount)) AS revenue
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY d.full_date
    ORDER BY d.full_date
""").set_index("full_date")
st.line_chart(daily)

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 10 pickup zones by revenue")
    top_zones = run_query("""
        SELECT z.zone, ROUND(SUM(f.total_amount)) AS revenue
        FROM fact_trips f
        JOIN dim_zone z ON f.pickup_zone_key = z.zone_key
        GROUP BY z.zone
        ORDER BY revenue DESC
        LIMIT 10
    """).set_index("zone")
    st.bar_chart(top_zones)

with col_right:
    st.subheader("Trips by payment type")
    payment_mix = run_query("""
        SELECT COALESCE(p.description, 'Unknown') AS payment_type, COUNT(*) AS trips
        FROM fact_trips f
        LEFT JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
        GROUP BY p.description
        ORDER BY trips DESC
    """).set_index("payment_type")
    st.bar_chart(payment_mix)
