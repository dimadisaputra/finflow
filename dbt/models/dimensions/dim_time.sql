{{
    config(
        materialized='table',
        tags=['v1']
    )
}}

-- dim_time.sql
-- Time dimension — generated from transaction timestamps.

WITH date_spine AS (
    SELECT DISTINCT
        DATE_TRUNC('day', transaction_timestamp)::DATE AS date_day
    FROM {{ ref('stg_transactions') }}
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day)       AS year,
    EXTRACT(MONTH FROM date_day)      AS month,
    EXTRACT(DAY FROM date_day)        AS day_of_month,
    EXTRACT(DOW FROM date_day)        AS day_of_week,
    EXTRACT(WEEK FROM date_day)       AS week_of_year,
    EXTRACT(QUARTER FROM date_day)    AS quarter,
    CASE
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                AS is_weekend,
    STRFTIME(date_day, '%B')          AS month_name,
    STRFTIME(date_day, '%A')          AS day_name
FROM date_spine
ORDER BY date_day
