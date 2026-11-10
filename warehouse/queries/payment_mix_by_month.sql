-- Payment type share of trips and revenue, per month.
-- Demonstrates: CTE + window function (SUM ... OVER PARTITION, no ORDER BY)
-- used to compute a percent-of-total alongside the grouped aggregate.

WITH monthly_payment AS (
    SELECT
        d.year,
        d.month,
        p.description AS payment_type,
        COUNT(*) AS trips,
        ROUND(SUM(f.total_amount), 2) AS revenue
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    LEFT JOIN dim_payment_type p ON f.payment_type_key = p.payment_type_key
    GROUP BY d.year, d.month, p.description
)
SELECT
    year,
    month,
    COALESCE(payment_type, 'Unknown') AS payment_type,
    trips,
    revenue,
    ROUND(100.0 * trips / SUM(trips) OVER (PARTITION BY year, month), 1) AS pct_of_trips,
    ROUND(100.0 * revenue / SUM(revenue) OVER (PARTITION BY year, month), 1) AS pct_of_revenue
FROM monthly_payment
ORDER BY year, month, trips DESC;
