-- Top 5 pickup zones by revenue, ranked within each month.
-- Demonstrates: CTE + RANK() window function partitioned by month.

WITH monthly_zone_revenue AS (
    SELECT
        d.year,
        d.month,
        z.zone,
        z.borough,
        ROUND(SUM(f.total_amount), 2) AS revenue,
        COUNT(*) AS trips
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    JOIN dim_zone z ON f.pickup_zone_key = z.zone_key
    GROUP BY d.year, d.month, z.zone, z.borough
),
ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY year, month ORDER BY revenue DESC) AS rev_rank
    FROM monthly_zone_revenue
)
SELECT year, month, zone, borough, revenue, trips, rev_rank
FROM ranked
WHERE rev_rank <= 5
ORDER BY year, month, rev_rank;
