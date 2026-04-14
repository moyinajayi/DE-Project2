-- staging/stg_chicago_crime.sql
-- Light cleaning and type casting from the raw BigQuery table.

{{ config(materialized='view') }}

SELECT
    CAST(id AS INT64)                          AS crime_id,
    case_number,
    CAST(date AS TIMESTAMP)                    AS crime_datetime,
    DATE(CAST(date AS TIMESTAMP))              AS crime_date,
    EXTRACT(YEAR FROM CAST(date AS TIMESTAMP)) AS year,
    EXTRACT(MONTH FROM CAST(date AS TIMESTAMP)) AS month,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP)) AS hour,
    block,
    primary_type                               AS crime_type,
    description                                AS crime_description,
    location_description,
    CAST(arrest AS BOOL)                       AS arrest,
    CAST(domestic AS BOOL)                     AS domestic,
    CAST(beat AS INT64)                        AS beat,
    CAST(district AS INT64)                    AS district,
    CAST(ward AS INT64)                        AS ward,
    CAST(community_area AS INT64)              AS community_area,
    CAST(latitude AS FLOAT64)                  AS latitude,
    CAST(longitude AS FLOAT64)                 AS longitude

FROM {{ source('staging', 'chicago_crime_raw') }}
WHERE primary_type IS NOT NULL
  AND date IS NOT NULL
