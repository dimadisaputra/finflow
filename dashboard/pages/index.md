# FinFlow Dashboard

Welcome to FinFlow — your personal finance aggregation and fraud detection dashboard.

```sql overview_stats
SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(amount) AS total_volume,
    COUNT(DISTINCT source) AS active_sources
FROM mart_monthly_cashflow
```

<BigValue
    data={overview_stats}
    value="total_transactions"
    title="Total Transactions"
/>

<BigValue
    data={overview_stats}
    value="unique_users"
    title="Unique Users"
/>

<BigValue
    data={overview_stats}
    value="total_volume"
    title="Total Volume (IDR)"
    fmt="num0"
/>

## Quick Links

- [Monthly Cashflow](/cashflow) — Revenue and spending trends
- [Fraud Alerts](/fraud) — Real-time fraud detection results
- [User Summary](/user) — Per-user transaction analysis

---

*Data refreshed daily at 02:00 UTC via dbt transformations.*
