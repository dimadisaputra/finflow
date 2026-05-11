# Monthly Cashflow

```sql cashflow_trend
SELECT
    month,
    source,
    transaction_count,
    total_amount,
    avg_amount
FROM finflow.mart_monthly_cashflow
ORDER BY month DESC
LIMIT 60
```

<LineChart
    data={cashflow_trend}
    x="month"
    y="total_amount"
    series="source"
    title="Monthly Transaction Volume by Source"
/>

<BarChart
    data={cashflow_trend}
    x="month"
    y="transaction_count"
    series="source"
    title="Monthly Transaction Count by Source"
/>

<DataTable data={cashflow_trend} />
