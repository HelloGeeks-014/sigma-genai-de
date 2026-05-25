SELECT
    t.merchant_id,
    SUM(t.amount) AS revenue,
    m.merchant_name,
    m.email
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_id = m.merchant_id
WHERE t.created_at > '2024-01-01'
GROUP BY t.merchant_id, m.merchant_name, m.email;