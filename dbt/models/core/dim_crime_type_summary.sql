-- core/dim_crime_type_summary.sql
-- Summary of total crimes by type for categorical distribution chart.
--
-- Clustering rationale:
--   CLUSTER BY crime_type: table is small (~35 rows), no partition needed.

{{
  config(
    materialized='table',
    cluster_by=["crime_type"],
    post_hook=[
      "CREATE SEARCH INDEX IF NOT EXISTS idx_dim_crime_type ON {{ this }} (crime_type)"
    ]
  )
}}

SELECT
    crime_type,
    COUNT(*)                                      AS total_incidents,
    COUNTIF(arrest)                               AS total_arrests,
    ROUND(SAFE_DIVIDE(COUNTIF(arrest), COUNT(*)) * 100, 1) AS arrest_rate_pct,
    MIN(crime_date)                               AS earliest_incident,
    MAX(crime_date)                               AS latest_incident,
    COUNT(DISTINCT district)                      AS districts_affected

FROM {{ ref('stg_chicago_crime') }}
GROUP BY crime_type
ORDER BY total_incidents DESC
