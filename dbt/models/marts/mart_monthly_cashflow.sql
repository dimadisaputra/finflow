{{
    config(
        materialized='table',
        tags=['v1']
    )
}}

-- mart_monthly_cashflow.sql
-- Monthly cashflow aggregation — monthly grain.

SELECT
    DATE_TRUNC('month', transaction_timestamp)  AS month,
    source,
    transaction_type,
    COUNT(*)                                     AS transaction_count,
    SUM(amount)                                  AS total_amount,
    AVG(amount)                                  AS avg_amount,
    MIN(amount)                                  AS min_amount,
    MAX(amount)                                  AS max_amount
FROM {{ ref('stg_transactions') }}
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3
