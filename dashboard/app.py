"""
Streamlit Dashboard: Chicago Crime Data Insights

Tiles:
  1. KPI metrics (total crimes, arrests, arrest rate, crime types)
  2. Crime Distribution by Type (bar chart)
  3. Arrest Rate by Crime Type (horizontal bar chart)
  4. Monthly Crime Trends Over Time (line chart)
  5. Year-over-Year Crime Totals (bar + YoY % change)
  6. Top Districts by Crime Volume (bar chart)
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
        SELECT crime_type, district, month_start,
               SUM(crime_count) AS crime_count,
               SUM(arrest_count) AS arrest_count
        FROM `{project_id}.chicago_crime.fact_crime_monthly`
        GROUP BY crime_type, district, month_start
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
df_type_filtered = df_type[df_type["crime_type"].isin(selected_types)]


# ── KPI Metrics ─────────────────────────────────────────────────────────
st.header("Key Metrics")

total_crimes = int(df_type_filtered["total_incidents"].sum())
total_arrests = int(df_type_filtered["total_arrests"].sum())
overall_arrest_rate = round(total_arrests / total_crimes * 100, 1) if total_crimes else 0
num_crime_types = len(df_type_filtered)

# YoY change: compare last full year to the year before
yearly = (
    df_monthly_filtered
    .groupby(df_monthly_filtered["month_start"].dt.year)["crime_count"]
    .sum()
    .sort_index()
)
if len(yearly) >= 2:
    last_year = yearly.iloc[-2]  # last full year (current year may be partial)
    prev_year = yearly.iloc[-3] if len(yearly) >= 3 else yearly.iloc[-2]
    yoy_change = round((last_year - prev_year) / prev_year * 100, 1) if prev_year else 0
else:
    yoy_change = 0

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Total Incidents", f"{total_crimes:,}")
kpi2.metric("Total Arrests", f"{total_arrests:,}")
kpi3.metric("Arrest Rate", f"{overall_arrest_rate}%")
kpi4.metric("YoY Change (last full yr)", f"{yoy_change:+.1f}%")


# ── Tile 1: Crime Distribution by Type ─────────────────────────────────
st.header("1 — Crime Distribution by Type")
st.markdown("Total incidents recorded per crime category (2001-present).")

st.bar_chart(
    df_type_filtered.set_index("crime_type")["total_incidents"],
    use_container_width=True,
)

with st.expander("View raw data"):
    st.dataframe(df_type_filtered, use_container_width=True)


# ── Tile 2: Arrest Rate by Crime Type ──────────────────────────────────
st.header("2 — Arrest Rate by Crime Type")
st.markdown("Percentage of incidents resulting in arrest, per category (calculated field).")

df_arrest = df_type_filtered[["crime_type", "arrest_rate_pct"]].copy()
df_arrest = df_arrest.sort_values("arrest_rate_pct", ascending=True)

st.bar_chart(
    df_arrest.set_index("crime_type")["arrest_rate_pct"],
    use_container_width=True,
    horizontal=True,
)


# ── Tile 3: Monthly Crime Trends Over Time ─────────────────────────────
st.header("3 — Monthly Crime Trends Over Time")
st.markdown("Crime volume by month, broken down by type.")

df_monthly_agg = (
    df_monthly_filtered
    .groupby(["crime_type", "month_start"])[["crime_count"]]
    .sum()
    .reset_index()
)
df_pivot = df_monthly_agg.pivot_table(
    index="month_start", columns="crime_type", values="crime_count", aggfunc="sum"
).fillna(0)

st.line_chart(df_pivot, use_container_width=True)

with st.expander("View raw data"):
    st.dataframe(df_monthly_filtered, use_container_width=True)


# ── Tile 4: Year-over-Year Crime Totals ────────────────────────────────
st.header("4 — Year-over-Year Crime Totals")
st.markdown("Annual crime volume with year-over-year percentage change (calculated field).")

df_yearly = (
    df_monthly_filtered
    .groupby(df_monthly_filtered["month_start"].dt.year)
    .agg(crime_count=("crime_count", "sum"), arrest_count=("arrest_count", "sum"))
    .reset_index()
    .rename(columns={"month_start": "year"})
)
df_yearly["yoy_pct_change"] = df_yearly["crime_count"].pct_change() * 100
df_yearly["yoy_pct_change"] = df_yearly["yoy_pct_change"].round(1)
df_yearly["arrest_rate_pct"] = (
    (df_yearly["arrest_count"] / df_yearly["crime_count"] * 100).round(1)
)

col_chart, col_table = st.columns([2, 1])
with col_chart:
    st.bar_chart(df_yearly.set_index("year")["crime_count"], use_container_width=True)
with col_table:
    st.dataframe(
        df_yearly[["year", "crime_count", "yoy_pct_change", "arrest_rate_pct"]].rename(
            columns={
                "year": "Year",
                "crime_count": "Crimes",
                "yoy_pct_change": "YoY %",
                "arrest_rate_pct": "Arrest %",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


# ── Tile 5: Top Districts by Crime Volume ──────────────────────────────
st.header("5 — Top Districts by Crime Volume")
st.markdown("Districts with the highest number of reported incidents.")

df_district = (
    df_monthly_filtered
    .groupby("district")
    .agg(crime_count=("crime_count", "sum"), arrest_count=("arrest_count", "sum"))
    .reset_index()
    .sort_values("crime_count", ascending=False)
    .head(15)
)
df_district["arrest_rate_pct"] = (
    (df_district["arrest_count"] / df_district["crime_count"] * 100).round(1)
)
df_district["district"] = df_district["district"].astype(str)

st.bar_chart(
    df_district.set_index("district")["crime_count"],
    use_container_width=True,
)

with st.expander("View district details"):
    st.dataframe(
        df_district.rename(columns={
            "district": "District",
            "crime_count": "Crimes",
            "arrest_count": "Arrests",
            "arrest_rate_pct": "Arrest %",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ── Footer ─────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Data source: City of Chicago Open Data Portal · "
    "Pipeline: Prefect + Kafka → GCS → BigQuery → dbt → Streamlit"
)
