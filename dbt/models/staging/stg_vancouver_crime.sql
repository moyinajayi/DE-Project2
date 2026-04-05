-- staging/stg_vancouver_crime.sql
-- Light cleaning and type casting from the raw BigQuery table.

{{ config(materialized='view') }}

SELECT
    TYPE                             AS crime_type,
    CAST(YEAR AS INT64)              AS year,
    CAST(MONTH AS INT64)             AS month,
    CAST(DAY AS INT64)               AS day,
    CAST(HOUR AS INT64)              AS hour,
    CAST(MINUTE AS INT64)            AS minute,
    HUNDRED_BLOCK                    AS hundred_block,
    NEIGHBOURHOOD                    AS neighbourhood,
    CAST(X AS FLOAT64)               AS longitude,
    CAST(Y AS FLOAT64)               AS latitude,
    -- Build a proper date from components
    DATE(CAST(YEAR AS INT64), CAST(MONTH AS INT64), CAST(DAY AS INT64)) AS crime_date

FROM {{ source('staging', 'vancouver_crime_raw') }}
WHERE TYPE IS NOT NULL
  AND YEAR IS NOT NULL
  AND MONTH IS NOT NULL
