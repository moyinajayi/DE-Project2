"""
Streamlit Dashboard: Vancouver Crime Data Insights

Tiles:
  1. Crime Distribution by Type (bar chart — categorical)
  2. Monthly Crime Trends Over Time (line chart — temporal)
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Vancouver Crime Dashboard",
    page_icon="🔍",
    layout="wide",
)

st.title("Vancouver Crime Data — Dashboard")
st.markdown(
    "Exploring crime patterns in Vancouver using open data from the "
    "[Vancouver Police Department](https://vpd.ca/)."
)


# ── BigQuery connection ─────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        st.secrets["gcp"]["credentials_path"],
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(credentials=credentials, project=st.secrets["gcp"]["project_id"])


client = get_bq_client()
project_id = st.secrets["gcp"]["project_id"]


# ── Data loading ────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_crime_type_summary() -> pd.DataFrame:
    query = f"""
        SELECT crime_type, total_incidents, neighbourhoods_affected
        FROM `{project_id}.vancouver_crime.dim_crime_type_summary`
        ORDER BY total_incidents DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_monthly_trends() -> pd.DataFrame:
    query = f"""
        SELECT crime_type, month_start, SUM(crime_count) AS crime_count
        FROM `{project_id}.vancouver_crime.fact_crime_monthly`
        GROUP BY crime_type, month_start
        ORDER BY month_start
    """
    df = client.query(query).to_dataframe()
    df["month_start"] = pd.to_datetime(df["month_start"])
    return df


df_type = load_crime_type_summary()
df_monthly = load_monthly_trends()


# ── Sidebar filters ─────────────────────────────────────────────────────────
st.sidebar.header("Filters")

all_types = df_monthly["crime_type"].unique().tolist()
selected_types = st.sidebar.multiselect(
    "Crime Types",
    options=all_types,
    default=all_types,
)

year_min = int(df_monthly["month_start"].dt.year.min())
year_max = int(df_monthly["month_start"].dt.year.max())
year_range = st.sidebar.slider("Year Range", year_min, year_max, (year_min, year_max))

# Apply filters to monthly data
mask = (
    df_monthly["crime_type"].isin(selected_types)
    & (df_monthly["month_start"].dt.year >= year_range[0])
    & (df_monthly["month_start"].dt.year <= year_range[1])
)
df_monthly_filtered = df_monthly[mask]


# ── Tile 1: Crime Distribution by Type ──────────────────────────────────────
st.header("1 — Crime Distribution by Type")
st.markdown("Total incidents recorded per crime category across all years.")

df_type_filtered = df_type[df_type["crime_type"].isin(selected_types)]

st.bar_chart(
    df_type_filtered.set_index("crime_type")["total_incidents"],
    use_container_width=True,
)

with st.expander("View raw data"):
    st.dataframe(df_type_filtered, use_container_width=True)


# ── Tile 2: Monthly Crime Trends Over Time ──────────────────────────────────
st.header("2 — Monthly Crime Trends Over Time")
st.markdown("Crime volume by month, broken down by type.")

# Pivot for multi-line chart
df_pivot = df_monthly_filtered.pivot_table(
    index="month_start", columns="crime_type", values="crime_count", aggfunc="sum"
).fillna(0)

st.line_chart(df_pivot, use_container_width=True)

with st.expander("View raw data"):
    st.dataframe(df_monthly_filtered, use_container_width=True)


# ── Footer ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Data source: Vancouver Police Department Open Data · "
    "Pipeline: Prefect → GCS → BigQuery → dbt → Streamlit"
)
