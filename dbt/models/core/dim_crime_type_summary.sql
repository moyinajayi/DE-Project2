-- core/dim_crime_type_summary.sql
-- Summary of total crimes by type, useful for the categorical distribution chart.
--
-- Clustering rationale:
--   CLUSTER BY crime_type:
--     This dimension table is small (~15 rows) so partitioning is unnecessary.
--     Clustering by crime_type aligns with the dashboard's primary categorical
--     chart which queries/sorts by crime_type. For small tables the benefit is
--     marginal, but it demonstrates best practice and avoids partition overhead
--     on tables below BigQuery's 1 GB partition threshold.

{{
  config(
    materialized='table',
    cluster_by=["crime_type"]
  )
}}

SELECT
    crime_type,
    COUNT(*) AS total_incidents,
    MIN(crime_date) AS earliest_incident,
    MAX(crime_date) AS latest_incident,
    COUNT(DISTINCT neighbourhood) AS neighbourhoods_affected

FROM {{ ref('stg_vancouver_crime') }}
GROUP BY crime_type
ORDER BY total_incidents DESC
