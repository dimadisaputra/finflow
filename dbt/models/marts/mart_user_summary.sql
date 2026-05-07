{{
    config(
        materialized='table',
        tags=['v1']
    )
}}

-- mart_user_summary.sql
-- Per-user summary — user grain.

SELECT
    user_id,
    COUNT(*)                                                AS total_transactions,
    COUNT(DISTINCT source)                                  AS source_count,
    SUM(amount)                                             AS total_amount,
    AVG(amount)                                             AS avg_amount,
    MIN(transaction_timestamp)                              AS first_transaction_at,
    MAX(transaction_timestamp)                              AS last_transaction_at,
    COUNT(DISTINCT location_city)                           AS unique_cities,
    COUNT(DISTINCT DATE_TRUNC('day', transaction_timestamp)) AS active_days
FROM {{ ref('stg_transactions') }}
GROUP BY 1
