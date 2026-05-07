{{
    config(
        materialized='view',
        tags=['v1']
    )
}}

-- stg_transactions.sql
-- Staging view over the Silver Iceberg transactions table.
-- Reads from MinIO via DuckDB's S3 API integration.

SELECT
    transaction_id,
    user_id,
    CAST(amount AS DECIMAL(18, 2))          AS amount,
    transaction_timestamp,
    location_city,
    source,
    schema_version,
    transaction_type,
    no_rekening,
    bca_terminal_id,
    mandiri_channel,
    gopay_merchant_id,
    payment_method,
    ovo_merchant_name,
    CAST(cashback_amount AS DECIMAL(18, 2)) AS cashback_amount,
    card_last_four,
    merchant_category_code,
    merchant_name,
    ingested_at
FROM {{ source('silver', 'transactions') }}
