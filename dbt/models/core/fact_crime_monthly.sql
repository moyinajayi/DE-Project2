-- core/fact_crime_monthly.sql
-- Aggregated monthly crime counts by type and neighbourhood for dashboard.
--
-- Partitioning & Clustering rationale:
--   PARTITION BY month_start (monthly granularity):
--     The dashboard's temporal chart queries filter/group by month. Partitioning
--     by month_start lets BigQuery scan only the relevant time slices, reducing
--     bytes processed and cost significantly for time-range filtered queries.
--   CLUSTER BY crime_type, neighbourhood:
--     Both the categorical bar chart (filters by crime_type) and neighbourhood
--     drill-downs benefit from clustering. BigQuery co-locates rows with the
--     same crime_type and neighbourhood, enabling block-level pruning on these
--     frequently filtered/grouped columns.

{{
  config(
    materialized='table',
    partition_by={
      "field": "month_start",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["crime_type", "neighbourhood"]
  )
}}

SELECT
    crime_type,
    neighbourhood,
    year,
    month,
    DATE(year, month, 1) AS month_start,
    COUNT(*) AS crime_count

FROM {{ ref('stg_vancouver_crime') }}
GROUP BY crime_type, neighbourhood, year, month
