"""
Streamlit Dashboard: Chicago Crime Data Insights

Tiles:
  1. Crime Distribution by Type (bar chart)
  2. Monthly Crime Trends Over Time (line chart)
"""

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Chicago Crime Dashboard",
    page_icon="🔍",
    layout="wide",
)

st.title("Chicago Crime Data — Dashboard")
st.markdown(
    "Exploring crime patterns in Chicago using open data from the "
    "[City of Chicago Data Portal](https://data.cityofchicago.org/)."
)


@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        st.secrets["gcp"]["credentials_path"],
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(credentials=credentials, project=st.secrets["gcp"]["project_id"])


client = get_bq_client()
project_id = st.secrets["gcp"]["project_id"]


@st.cache_data(ttl=3600)
def load_crime_type_summary() -> pd.DataFrame:
    query = f"""
        SELECT crime_type, total_incidents, total_arrests, arrest_rate_pct, districts_affected
        FROM `{project_id}.chicago_crime.dim_crime_type_summary`
        ORDER BY total_incidents DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_monthly_trends() -> pd.DataFrame:
    query = f"""
        SELECT crime_type, month_start, SUM(crime_count) AS crime_count,
               SUM(arrest_count) AS arrest_count
        FROM `{project_id}.chicago_crime.fact_crime_monthly`
        GROUP BY crime_type, month_start
        ORDER BY month_start
    """
    df = client.query(query).to_dataframe()
    df["month_start"] = pd.to_datetime(df["month_start"])
    return df


df_type = load_crime_type_summary()
df_monthly = load_monthly_trends()


# ── Sidebar filters ────────────────────────────────────────────────────
st.sidebar.header("Filters")

all_types = sorted(df_monthly["crime_type"].unique().tolist())
top_types = df_type.head(10)["crime_type"].tolist()
selected_types = st.sidebar.multiselect(
    "Crime Types",
    options=all_types,
    default=top_types,
)

year_min = int(df_monthly["month_start"].dt.year.min())
year_max = int(df_monthly["month_start"].dt.year.max())
year_range = st.sidebar.slider("Year Range", year_min, year_max, (year_min, year_max))

mask = (
    df_monthly["crime_type"].isin(selected_types)
    & (df_monthly["month_start"].dt.year >= year_range[0])
    & (df_monthly["month_start"].dt.year <= year_range[1])
)
df_monthly_filtered = df_monthly[mask]


# ── Tile 1: Crime Distribution by Type ─────────────────────────────────
st.header("1 — Crime Distribution by Type")
st.markdown("Total incidents recorded per crime category (2001-present).")

df_type_filtered = df_type[df_type["crime_type"].isin(selected_types)]

st.bar_chart(
    df_type_filtered.set_index("crime_type")["total_incidents"],
    use_container_width=True,
)

with st.expander("View raw data"):
    st.dataframe(df_type_filtered, use_container_width=True)


# ── Tile 2: Monthly Crime Trends Over Time ─────────────────────────────
st.header("2 — Monthly Crime Trends Over Time")
st.markdown("Crime volume by month, broken down by type.")

df_pivot = df_monthly_filtered.pivot_table(
    index="month_start", columns="crime_type", values="crime_count", aggfunc="sum"
).fillna(0)

st.line_chart(df_pivot, use_container_width=True)

with st.expander("View raw data"):
    st.dataframe(df_monthly_filtered, use_container_width=True)


# ── Footer ─────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Data source: City of Chicago Open Data Portal · "
    "Pipeline: Prefect + Kafka -> GCS -> BigQuery -> dbt -> Streamlit"
)
