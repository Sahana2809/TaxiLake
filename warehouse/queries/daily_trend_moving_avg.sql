-- Daily trip count and revenue, with a trailing 7-day moving average.
-- Demonstrates: window function with a frame (ROWS BETWEEN ... PRECEDING).

WITH daily AS (
    SELECT
        d.full_date,
        COUNT(*) AS trips,
        ROUND(SUM(f.total_amount), 2) AS revenue
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY d.full_date
)
SELECT
    full_date,
    trips,
    revenue,
    ROUND(AVG(trips) OVER (
        ORDER BY full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )) AS trips_7day_avg,
    ROUND(AVG(revenue) OVER (
        ORDER BY full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS revenue_7day_avg
FROM daily
ORDER BY full_date;
