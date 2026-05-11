# Fraud Alerts

```sql fraud_summary
SELECT
    DATE_TRUNC('day', detected_at) AS alert_date,
    COUNT(*) AS alert_count,
    AVG(txn_count) AS avg_txn_per_alert,
    AVG(total_amount) AS avg_amount_per_alert
FROM finflow.mart_fraud_alerts
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30
```

<LineChart
    data={fraud_summary}
    x="alert_date"
    y="alert_count"
    title="Daily Fraud Alerts"
/>

```sql recent_alerts
SELECT
    user_id,
    alert_type,
    txn_count,
    total_amount,
    window_duration_minutes,
    detected_at
FROM finflow.mart_fraud_alerts
ORDER BY detected_at DESC
LIMIT 50
```

<DataTable
    data={recent_alerts}
    title="Recent Fraud Alerts"
/>
