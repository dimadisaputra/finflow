{{
    config(
        materialized='table',
        tags=['v1']
    )
}}

-- mart_fraud_alerts.sql
-- Gold fraud alerts reporting layer — per-alert grain.
-- Reads from the Gold Iceberg table (via alerts_sink.py), never from Redpanda directly.

SELECT
    user_id,
    window_start,
    window_end,
    txn_count,
    total_amount,
    alert_type,
    detected_at,
    DATEDIFF('minute', window_start, window_end)  AS window_duration_minutes
FROM {{ source('gold', 'fraud_alerts') }}
ORDER BY detected_at DESC
