# User Summary

```sql top_users
SELECT
    user_id,
    total_transactions,
    source_count,
    total_amount,
    avg_amount,
    unique_cities,
    active_days,
    first_transaction_at,
    last_transaction_at
FROM mart_user_summary
ORDER BY total_amount DESC
LIMIT 100
```

<BarChart
    data={top_users}
    x="user_id"
    y="total_amount"
    title="Top Users by Transaction Volume"
/>

```sql user_activity
SELECT
    source_count AS sources_used,
    COUNT(*) AS user_count,
    AVG(total_transactions) AS avg_transactions
FROM mart_user_summary
GROUP BY 1
ORDER BY 1
```

<BarChart
    data={user_activity}
    x="sources_used"
    y="user_count"
    title="Users by Number of Sources Used"
/>

<DataTable data={top_users} title="User Details" />
