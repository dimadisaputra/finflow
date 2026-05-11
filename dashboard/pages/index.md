# FinFlow Dashboard

Welcome to FinFlow — your personal finance aggregation and fraud detection dashboard.

```sql overview_stats
SELECT
    (SELECT SUM(total_transactions) FROM finflow.mart_user_summary) AS total_transactions,
    (SELECT COUNT(*) FROM finflow.mart_user_summary) AS unique_users,
    (SELECT SUM(total_amount) FROM finflow.mart_user_summary) AS total_volume,
    (SELECT COUNT(DISTINCT source) FROM finflow.mart_monthly_cashflow) AS active_sources
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
