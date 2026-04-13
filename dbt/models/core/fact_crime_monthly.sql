-- core/fact_crime_monthly.sql
-- Aggregated monthly crime counts by type and district for dashboard.
--
-- Partitioning & Clustering rationale:
--   PARTITION BY month_start (monthly):
--     Dashboard temporal chart filters by month. Partitioning reduces scanned
--     data for time-range queries.
--   CLUSTER BY crime_type, district:
--     Bar chart groups by crime_type; district drill-downs filter by district.
--     Clustering enables block-level pruning on these columns.

{{
  config(
    materialized='table',
    partition_by={
      "field": "month_start",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["crime_type", "district"],
    post_hook=[
      "CREATE SEARCH INDEX IF NOT EXISTS idx_fact_crime_type ON {{ this }} (crime_type)"
    ]
  )
}}

SELECT
    crime_type,
    district,
    year,
    month,
    DATE(year, month, 1)                         AS month_start,
    COUNT(*)                                      AS crime_count,
    COUNTIF(arrest)                               AS arrest_count,
    ROUND(SAFE_DIVIDE(COUNTIF(arrest), COUNT(*)) * 100, 1) AS arrest_rate_pct

FROM {{ ref('stg_chicago_crime') }}
GROUP BY crime_type, district, year, month
