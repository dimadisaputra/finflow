{{
    config(
        materialized='table',
        tags=['v1']
    )
}}

-- dim_user.sql
-- User dimension — derived from transaction data.

SELECT DISTINCT
    user_id,
    MIN(transaction_timestamp)                   AS first_seen_at,
    MAX(transaction_timestamp)                   AS last_seen_at,
    COUNT(DISTINCT source)                       AS source_count,
    COUNT(DISTINCT location_city)                AS city_count
FROM {{ ref('stg_transactions') }}
GROUP BY 1
