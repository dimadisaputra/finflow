{{
    config(
        materialized='table',
        tags=['v1']
    )
}}

-- dim_source.sql
-- Source dimension — one row per transaction source.

SELECT DISTINCT
    source                                       AS source_id,
    source                                       AS source_name,
    CASE source
        WHEN 'bca'     THEN 'Bank Transfer'
        WHEN 'mandiri' THEN 'Bank Transfer'
        WHEN 'gopay'   THEN 'E-Wallet'
        WHEN 'ovo'     THEN 'E-Wallet'
        WHEN 'visa'    THEN 'Credit Card'
        ELSE 'Unknown'
    END                                          AS source_category,
    MIN(transaction_timestamp)                   AS first_transaction_at,
    MAX(transaction_timestamp)                   AS last_transaction_at,
    COUNT(*)                                     AS total_transactions
FROM {{ ref('stg_transactions') }}
GROUP BY 1, 2, 3
